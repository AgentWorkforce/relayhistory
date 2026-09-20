use anyhow::{Context, Result};
#[cfg(feature = "opencode-backup")]
use rusqlite::DatabaseName;
use rusqlite::{
    params, Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use crate::relationship_graph::{
    relationship_capabilities, session_children, session_children_page, session_continuity_edges,
    session_parents, session_relationships, session_tree, RelationshipCapabilities,
    RelationshipCursor, RelationshipDiagnostic, RelationshipKinds, SessionChildrenPage,
    SessionRelationship, SessionRelationships, SessionTree, SessionTreeNode, SessionTreeOptions,
    CONTINUITY_RELATIONSHIPS, DEFAULT_CHILDREN_PAGE_LIMIT, DEFAULT_TREE_MAX_DEPTH,
    DEFAULT_TREE_MAX_NODES, MAX_CHILDREN_PAGE_LIMIT, MAX_TREE_MAX_DEPTH, MAX_TREE_MAX_NODES,
    RELATIONSHIP_CONTINUATION, RELATIONSHIP_FORK, RELATIONSHIP_RESUME,
    SESSION_RELATIONSHIP_CONTRACT_VERSION,
};

pub const SOURCE_CHOICES: &[&str] = &[
    "claude",
    "codex",
    "cursor",
    "grok",
    "relay",
    "trajectory",
    "opencode",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryEntry {
    #[serde(default)]
    pub id: i64,
    pub source: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
    pub prompt: String,
    #[serde(default)]
    pub prompt_hash: Option<String>,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tag {
    pub name: String,
    pub display_name: String,
    pub color: Option<String>,
    pub session_count: i64,
    pub first_tagged_ms: Option<i64>,
    pub last_tagged_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaggedSession {
    pub source: String,
    pub session_id: String,
    pub project: Option<String>,
    pub entry_count: i64,
    pub last_activity_ms: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryFilter {
    pub source: Option<String>,
    pub project: Option<String>,
    pub tag: Option<String>,
    pub before_ms: Option<i64>,
    pub limit: i64,
    pub scope: SessionScope,
}

/// Which acquisition surface a query should include.
///
/// Local remains the default so upgrading does not turn an offline read into a
/// network-backed workflow or expose remotely discovered catalog entries in
/// existing commands. `All` is the union of both locations, not a third place.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SessionScope {
    #[default]
    Local,
    Remote,
    All,
}

/// A place where one logical provider session has been observed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SessionLocation {
    Local,
    Remote,
}

impl SessionLocation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

/// How [`stats_scoped_by`] buckets `by_project`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectGrouping {
    /// The canonical `project_key`, falling back to the raw `history.project`
    /// string for a row whose session has not been resolved yet. This is the
    /// default because it is the grouping that merges `/Users/a/proj` and
    /// `/home/b/proj` when they are the same repository.
    #[default]
    ProjectKey,
    /// The historical grouping: `history.project`, which is the raw cwd.
    Cwd,
}

impl ProjectGrouping {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProjectKey => "project_key",
            Self::Cwd => "cwd",
        }
    }

    /// The grouping expression, against `history` aliased as `h`.
    ///
    /// A correlated subquery rather than a join: it is answered by the
    /// `sessions` primary key, and a join would change the row count if the
    /// catalog ever held a duplicate. The `COALESCE` matters -- a history row
    /// whose session has no resolved key still has to appear somewhere, and
    /// dropping it would leave a total that does not match the sum of the
    /// buckets.
    pub fn expression(self) -> &'static str {
        match self {
            Self::ProjectKey => {
                "COALESCE((SELECT s.project_key FROM sessions s \
                  WHERE s.source = h.source AND s.session_id = h.session_id), h.project)"
            }
            Self::Cwd => "h.project",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Stats {
    pub total: i64,
    pub by_source: Vec<(String, i64)>,
    /// Top projects, bucketed by [`Stats::grouping`].
    pub by_project: Vec<(String, i64)>,
    /// Which key `by_project` is bucketed by.
    #[serde(default)]
    pub grouping: ProjectGrouping,
    pub first_timestamp_ms: Option<i64>,
    pub last_timestamp_ms: Option<i64>,
}

pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT,
    project TEXT,
    prompt TEXT NOT NULL,
    prompt_hash TEXT,
    timestamp_ms INTEGER NOT NULL,
    UNIQUE(source, timestamp_ms, prompt)
);
CREATE VIRTUAL TABLE IF NOT EXISTS history_fts USING fts5(
    prompt, project, content='history', content_rowid='id'
);
CREATE TABLE IF NOT EXISTS session_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    project TEXT,
    project_key TEXT,
    project_key_method TEXT,
    cwd TEXT,
    git_branch TEXT,
    message_id TEXT,
    parent_id TEXT,
    ts_ms INTEGER NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('user', 'assistant', 'tool_result')),
    kind TEXT NOT NULL CHECK(kind IN ('text', 'thinking', 'tool_use', 'tool_result')),
    text TEXT,
    model TEXT,
    token_json TEXT,
    -- The upstream inference provider, when the harness records it as its own
    -- field. OpenCode writes `providerID`; Claude and Codex do not name one,
    -- so theirs stays null rather than being guessed from the model string.
    provider TEXT,
    event_uid TEXT NOT NULL,
    tool_use_id TEXT,
    payload_bytes INTEGER,
    payload_truncated INTEGER,
    payload_hash TEXT,
    call_index INTEGER,
    event_index INTEGER,
    result_status TEXT,
    event_source TEXT,
    error_signal TEXT,
    subagent_session_id TEXT,
    agent_id TEXT,
    request_id TEXT,
    -- Why the turn ended, as the harness itself reported it.
    stop_reason TEXT,
    agent_version TEXT,
    is_sidechain INTEGER,
    is_meta INTEGER,
    turn_id TEXT,
    raw_facts_version INTEGER,
    UNIQUE(source, session_id, event_uid)
);
CREATE VIRTUAL TABLE IF NOT EXISTS session_events_fts USING fts5(
    text, role, project, content='session_events', content_rowid='id'
);
-- Points in a session that are not turns: a context compaction, a resume, a
-- fork. `marker_uid` is derived from the provider record that established the
-- marker, so re-ingesting the same session is idempotent.
CREATE TABLE IF NOT EXISTS session_markers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    message_id TEXT,
    ts_ms INTEGER,
    text TEXT,
    detail_json TEXT,
    marker_uid TEXT NOT NULL,
    UNIQUE(source, session_id, marker_uid)
);
CREATE TABLE IF NOT EXISTS tool_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id TEXT,
    tool_use_id TEXT NOT NULL,
    name TEXT NOT NULL,
    target TEXT,
    args_json TEXT,
    is_error INTEGER,
    ts_ms INTEGER,
    UNIQUE(source, session_id, tool_use_id)
);
CREATE TABLE IF NOT EXISTS file_edits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id TEXT,
    tool_use_id TEXT NOT NULL,
    file_path TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    lines_added INTEGER,
    lines_removed INTEGER,
    structured_patch_json TEXT,
    user_modified INTEGER,
    ts_ms INTEGER,
    git_branch TEXT,
    cwd TEXT,
    UNIQUE(source, session_id, tool_use_id)
);
CREATE TABLE IF NOT EXISTS session_commit_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    repo TEXT NOT NULL,
    branch TEXT,
    commit_sha TEXT NOT NULL,
    note_ref TEXT,
    match_method TEXT NOT NULL,
    confidence REAL NOT NULL,
    files_json TEXT,
    numstat_json TEXT,
    evidence_json TEXT,
    created_at_ms INTEGER NOT NULL,
    UNIQUE(source, session_id, commit_sha, match_method)
);
CREATE TABLE IF NOT EXISTS trajectories (
    id TEXT PRIMARY KEY,
    version INTEGER,
    persona_id TEXT,
    project_id TEXT,
    task_title TEXT,
    task_description TEXT,
    status TEXT,
    started_at TEXT,
    completed_at TEXT,
    decisions_json TEXT NOT NULL,
    retrospective_json TEXT NOT NULL,
    search_text TEXT NOT NULL,
    path TEXT,
    updated_ms INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL
);
CREATE VIRTUAL TABLE IF NOT EXISTS trajectory_fts USING fts5(
    search_text, task_title, task_description, persona_id, project_id,
    content='trajectories', content_rowid='rowid'
);
CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    color TEXT,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS session_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    created_ms INTEGER NOT NULL,
    UNIQUE(source, session_id, tag_id)
);
CREATE TRIGGER IF NOT EXISTS history_ai AFTER INSERT ON history BEGIN
    INSERT INTO history_fts(rowid, prompt, project)
    VALUES (new.id, new.prompt, new.project);
END;
CREATE TRIGGER IF NOT EXISTS history_au AFTER UPDATE ON history BEGIN
    INSERT INTO history_fts(history_fts, rowid, prompt, project)
    VALUES('delete', old.id, old.prompt, old.project);
    INSERT INTO history_fts(rowid, prompt, project)
    VALUES (new.id, new.prompt, new.project);
END;
CREATE TRIGGER IF NOT EXISTS history_ad AFTER DELETE ON history BEGIN
    INSERT INTO history_fts(history_fts, rowid, prompt, project)
    VALUES('delete', old.id, old.prompt, old.project);
END;
CREATE TRIGGER IF NOT EXISTS session_events_ai AFTER INSERT ON session_events BEGIN
    INSERT INTO session_events_fts(rowid, text, role, project)
    VALUES (new.id, new.text, new.role, new.project);
END;
CREATE TRIGGER IF NOT EXISTS session_events_au AFTER UPDATE ON session_events BEGIN
    INSERT INTO session_events_fts(session_events_fts, rowid, text, role, project)
    VALUES('delete', old.id, old.text, old.role, old.project);
    INSERT INTO session_events_fts(rowid, text, role, project)
    VALUES (new.id, new.text, new.role, new.project);
END;
CREATE TRIGGER IF NOT EXISTS session_events_ad AFTER DELETE ON session_events BEGIN
    INSERT INTO session_events_fts(session_events_fts, rowid, text, role, project)
    VALUES('delete', old.id, old.text, old.role, old.project);
END;
"#;

pub fn default_db_path() -> PathBuf {
    std::env::var_os("AI_HIST_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            if let Some(xdg_data_home) = std::env::var_os("XDG_DATA_HOME") {
                return PathBuf::from(xdg_data_home).join("ai-hist/ai-history.db");
            }
            let home = std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));
            home.join(".local/share/ai-hist/ai-history.db")
        })
}

/// Maximum number of times SQLite asks us to retry a contended lock.
///
/// A busy handler is preferable to one flat timeout for normal writer overlap:
/// exponential delays stop hammering the lock and per-process/time jitter keeps
/// two scheduled syncs from waking and colliding in lockstep. The callback is
/// never useful for a permanently suspended holder, so the sequence stays
/// bounded while preserving the previous 30-second grace window for a healthy writer that is
/// merely slow. Even with zero jitter the sequence waits at least 30 seconds; jitter can extend
/// that to roughly 33 seconds so simultaneous syncs do not keep waking in lockstep.
const BUSY_RETRY_ATTEMPTS: i32 = 65;
const BUSY_RETRY_BASE_MS: u64 = 10;
const BUSY_RETRY_CAP_MS: u64 = 500;
const BUSY_RETRY_JITTER_DIVISOR: u64 = 10;
const JOURNAL_MODE_RETRY_ATTEMPTS: i32 = 20;
const JOURNAL_MODE_RETRY_MS: u64 = 10;

fn busy_retry_backoff_ms(prior_attempts: i32) -> Option<u64> {
    if !(0..BUSY_RETRY_ATTEMPTS).contains(&prior_attempts) {
        return None;
    }

    let shift = (prior_attempts as u32).min(6);
    Some(
        BUSY_RETRY_BASE_MS
            .saturating_mul(1_u64 << shift)
            .min(BUSY_RETRY_CAP_MS),
    )
}

fn busy_retry_handler(prior_attempts: i32) -> bool {
    let Some(backoff_ms) = busy_retry_backoff_ms(prior_attempts) else {
        return false;
    };
    let clock = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let seed = clock ^ u64::from(std::process::id()) ^ prior_attempts as u64;
    let jitter_ms = seed % (backoff_ms / BUSY_RETRY_JITTER_DIVISOR + 1);
    std::thread::sleep(Duration::from_millis(backoff_ms + jitter_ms));
    true
}

fn configure_busy_retry(conn: &Connection) -> Result<()> {
    conn.busy_handler(Some(busy_retry_handler))?;
    Ok(())
}

fn sqlite_lock_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

fn enable_wal_for_migration(conn: &Connection) -> Result<()> {
    for attempt in 0..=JOURNAL_MODE_RETRY_ATTEMPTS {
        match conn.pragma_update(None, "journal_mode", "WAL") {
            Ok(()) => return Ok(()),
            Err(error) if attempt < JOURNAL_MODE_RETRY_ATTEMPTS && sqlite_lock_error(&error) => {
                // Changing journal mode can return SQLITE_BUSY without invoking
                // the connection busy handler. This short retry only bridges
                // simultaneous first opens; the migration write lock below has
                // the normal bounded contention policy.
                std::thread::sleep(Duration::from_millis(JOURNAL_MODE_RETRY_MS));
            }
            Err(error) => return Err(error.into()),
        }
    }
    unreachable!("the bounded journal-mode retry loop always returns")
}

pub fn open_db(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    // Before init_db: creating the schema takes a write lock too.
    configure_busy_retry(&conn)?;
    init_db(&conn)?;
    Ok(conn)
}

/// An operation against an attached/source SQLite database failed. Keeping the source path in
/// the error chain lets callers diagnose that database rather than incorrectly probing the
/// destination history store.
#[derive(Debug)]
pub struct SourceDatabaseError {
    path: PathBuf,
    source: rusqlite::Error,
}

impl SourceDatabaseError {
    pub fn new(path: impl Into<PathBuf>, source: rusqlite::Error) -> Self {
        Self {
            path: path.into(),
            source,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl std::fmt::Display for SourceDatabaseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "reading source database {}: {}",
            self.path.display(),
            self.source
        )
    }
}

impl std::error::Error for SourceDatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Objects and columns [`init_db`] adds, checked before trusting a read-only
/// handle. Extend this whenever init_db gains a table or column, otherwise a
/// database created by an older release is served queries against a schema it
/// does not have.
const REQUIRED_TABLES: &[&str] = &[
    "history",
    "history_fts",
    "session_events",
    "session_events_fts",
    "session_markers",
    "tool_calls",
    "file_edits",
    "session_commit_links",
    "trajectories",
    "trajectory_fts",
    "tags",
    "session_tags",
    "sessions",
    "session_presences",
    "session_hydration_checkpoints",
    "session_identity_correlations",
    "session_relationships",
    "session_markers",
    "session_continuity_evidence",
    "schema_migrations",
    "discovery_skips",
];
#[cfg(feature = "delivery")]
const REQUIRED_DELIVERY_TABLES: &[&str] = &[
    "delivery_state",
    "delivery_jobs",
    "delivery_journal",
    "delivery_shadow",
    "delivery_batches",
    "delivery_bootstrap_bounds",
    "delivery_exclusions",
    "history_exports",
    "history_export_pages",
];
#[cfg(not(feature = "delivery"))]
const REQUIRED_DELIVERY_TABLES: &[&str] = &[];
const REQUIRED_HISTORY_COLUMNS: &[&str] = &["prompt_hash", "git_branch"];
/// Columns [`init_db`] adds to `sessions` after the original DDL. The shallow
/// session catalog (`ai-hist sessions list` / `discover`) reads every one of
/// them, so a read-only handle over a database that predates them would fail
/// with `no such column` instead of migrating.
const REQUIRED_SESSIONS_COLUMNS: &[&str] = &[
    "first_prompt",
    "models_json",
    "originator",
    "agent_version",
    "repo_url",
    "initial_commit",
    "workspace_roots_json",
    "source_stamp",
    "discovery_state",
    // Canonical project identity (issue #175). Every catalog read selects
    // both, so a database that predates them must migrate before a read-only
    // handle can serve one.
    "project_key",
    "project_key_method",
];
/// What a provider's parser reported about the session, kept with the
/// checkpoint so a later read that parses nothing can still report it. A
/// diagnostic like "this harness records no billing tokens" is a standing
/// fact about the stored evidence, not about the run that happened to parse
/// it.
const REQUIRED_HYDRATION_CHECKPOINT_COLUMNS: &[&str] = &["source_diagnostics_json"];
const REQUIRED_SESSION_PRESENCE_COLUMNS: &[&str] =
    &["raw_locator", "source_stamp", "discovery_state"];
/// The two marker producers landed independently: OpenCode records the
/// enclosing message while Grok records a displayable marker excerpt. A
/// database created by either implementation must gain the other column.
const REQUIRED_SESSION_MARKER_COLUMNS: &[(&str, &str)] =
    &[("message_id", "TEXT"), ("text", "TEXT")];
/// Columns [`init_db`] adds to `session_events` after the original DDL, with
/// the SQL type each one is added as.
///
/// Canonical project identity (issue #175): `project_key` is denormalized onto
/// the event so a grouping query does not have to join `sessions` for every
/// row, and `project_key_method` says what kind of key it is. The method is
/// storage, not surface: nothing selects it into [`crate::SessionEvent`]. It
/// exists so the denormalizing pass can rank what it is about to write against
/// what the row already holds, which is the only way to tell a key a delegated
/// child resolved for itself from one it was lent. Without it that pass
/// overwrote the former with the latter.
///
/// Then the per-message raw provider facts: the envelope facts a harness
/// records once per API request or per message and that normalization used to
/// read past. burn groups turns into API requests by `request_id`, detects an
/// in-progress turn by the *absence* of `stop_reason`, attributes sidechain
/// output, and bounds a Codex turn by `turn_id`. Every one is nullable, and
/// the wire value is stored verbatim -- relayhistory preserves, consumers map.
///
/// As with the catalog columns above, this list is both the migration list and
/// the read-only guard list, so the two cannot drift.
const REQUIRED_SESSION_EVENT_COLUMNS: &[(&str, &str)] = &[
    ("project_key", "TEXT"),
    ("project_key_method", "TEXT"),
    ("provider", "TEXT"),
    ("request_id", "TEXT"),
    ("stop_reason", "TEXT"),
    ("agent_version", "TEXT"),
    ("is_sidechain", "INTEGER"),
    ("is_meta", "INTEGER"),
    ("turn_id", "TEXT"),
    // Not a provider fact: the generation of raw-fact parsing the local parser
    // wrote the row with. It is the only field stamped on every event the
    // parser writes, whatever the provider recorded, which is what lets a
    // plain `sync` tell a row indexed before the facts existed from one whose
    // facts the provider genuinely never recorded. Adapter-supplied rows do
    // not carry it (it is absent from the session-event evidence spec), so the
    // probes that read it exclude sessions with remote provenance.
    ("raw_facts_version", "INTEGER"),
];
/// Columns the v2 `session_relationships` shape adds. A v1 row set cannot
/// represent related evidence whose child has no provider-recorded identity,
/// so a database still carrying the v1 table is not current for any read.
const REQUIRED_SESSION_RELATIONSHIP_COLUMNS: &[&str] = &[
    "relationship_uid",
    "identity_status",
    "evidence_kind",
    "child_has_events",
    // Continuity: the conversation a fork or continuation branched from,
    // when the provider named one distinct from the parent.
    "origin_session_id",
];

/// Every index a fast path depends on, across all of them.
///
/// The pre-existing guard checks tables and columns only. These are listed
/// because each one carries a promise some read makes: the two recency indexes
/// make a catalog listing an indexed, sort-free read, `idx_sessions_raw_path`
/// makes discovery's "has this transcript changed?" lookup a search rather than
/// a scan of every session on every candidate, and the event and evidence page
/// indexes carry their pagination's ordering. A database that somehow has the
/// columns but not an index would otherwise be served with silently degraded
/// plans. Missing means "not current", which routes the caller through the
/// writable open that recreates them.
///
/// This is the *writable* bar: [`init_db`]'s lock-free fast path skips the
/// migration only when every one is present. Read paths are each gated on
/// their own scoped subset below, so a database missing one index keeps its
/// read-only handle for every read that does not need it.
///
/// The older `idx_sessions_cwd` / `idx_sessions_branch` / `idx_sessions_last` /
/// `idx_sessions_source_last` are deliberately absent: nothing in the catalog
/// path depends on them any more.
const REQUIRED_INDEXES: &[&str] = &[
    "idx_sessions_recency",
    "idx_sessions_source_recency",
    "idx_sessions_raw_path",
    "idx_session_events_source_page",
    "idx_session_events_page",
    "idx_tool_calls_page_v2",
    "idx_file_edits_page_v2",
    "idx_session_presences_location",
    "idx_session_presences_locator",
    "idx_session_relationships_parent",
    "idx_session_relationships_child",
    // Continuity reconciliation resolves a transcript's first parent uuid
    // against the record that carries it, across every session. Without this
    // that is a scan of every event on every hydration.
    "idx_session_events_message",
    "idx_session_continuity_parent_uuid",
    "idx_session_continuity_pending",
    "idx_sessions_project_key",
];

/// Indexes no longer created: nothing queries them, or a replacement covers
/// strictly more, and each was one more btree per write. [`init_db`] drops
/// them so existing databases shed the write amplification too; while one is
/// still present the database has outstanding migration work, and the
/// lock-free fast path must not run.
///
/// The `sessions` four were never read by the catalog. The evidence four are
/// prefixes of, or superseded by, the `_v2` page indexes: `(source,
/// session_id)` is a strict prefix of the page index, and the original page
/// indexes ordered on the bare `ts_ms` column, which cannot serve the
/// canonical `ts_ms IS NULL, ts_ms, id` order. Because the schema guard
/// compares index *names*, the corrected shape had to take a new name — a
/// same-named old index would otherwise be read as current.
const RETIRED_INDEXES: &[&str] = &[
    "idx_sessions_cwd",
    "idx_sessions_branch",
    "idx_sessions_last",
    "idx_sessions_source_last",
    "idx_tool_calls_session",
    "idx_file_edits_session",
    "idx_tool_calls_page",
    "idx_file_edits_page",
];

const REQUIRED_CATALOG_READ_INDEXES: &[&str] = &[
    "idx_sessions_recency",
    "idx_sessions_source_recency",
    "idx_session_presences_location",
];
const REQUIRED_EVENT_READ_INDEXES: &[&str] =
    &["idx_session_events_source_page", "idx_session_events_page"];
/// Indexes the session-scoped tool call and file edit pages depend on. Both
/// pages always name one source and one session, so a single composite index
/// per table carries the whole lookup and its ordering.
const REQUIRED_EVIDENCE_READ_INDEXES: &[&str] =
    &["idx_tool_calls_page_v2", "idx_file_edits_page_v2"];
const REQUIRED_SCOPE_READ_INDEXES: &[&str] = &["idx_session_presences_location"];
const REQUIRED_RELATIONSHIP_READ_INDEXES: &[&str] = &[
    "idx_session_relationships_parent",
    "idx_session_relationships_child",
    // `getSessionRelationships` reports still-unresolved continuity evidence
    // for the session it was asked about.
    "idx_session_continuity_pending",
];
const REQUIRED_TRIGGERS: &[&str] = &[
    "delete_session_presences",
    "delete_session_hydration_state",
    "delete_session_identity_correlations",
    "delete_session_markers",
    "delete_session_continuity_evidence",
];
const REQUIRED_SCHEMA_MIGRATIONS: &[&str] = &[
    "session_presences_local_backfill_v1",
    "session_relationships_v2",
    "session_markers_v1",
    "session_markers_combined_v1",
    "session_events_tool_result_fidelity_v1",
    // `delete_session_hydration_state` gained a step, and `CREATE TRIGGER IF
    // NOT EXISTS` would otherwise leave an existing database on the old body
    // forever. The marker is what makes the rebuild happen exactly once.
    "session_delete_continuity_reopen_v1",
    "session_events_raw_facts_v1",
    "session_project_key_v1",
];
#[cfg(feature = "delivery")]
const REQUIRED_DELIVERY_MIGRATIONS: &[&str] = &["delivery_v1"];
#[cfg(not(feature = "delivery"))]
const REQUIRED_DELIVERY_MIGRATIONS: &[&str] = &[];

/// Whether this database already has everything [`init_db`] would add.
///
/// Read-only handles skip `init_db`, so an older database would otherwise be
/// queried against tables and columns that do not exist -- a user upgrading
/// from before `session_events` existed would get `no such table` on their
/// first search instead of a silent migration. Callers fall back to a writable
/// open (which migrates) when this returns false.
pub fn schema_is_current(conn: &Connection) -> Result<bool> {
    Ok(schema_has_required_indexes(conn, REQUIRED_INDEXES)?
        && delivery_schema_is_current(conn)?
        && crate::observations::schema_is_current(conn)?)
}

#[cfg(feature = "delivery")]
fn delivery_schema_is_current(conn: &Connection) -> Result<bool> {
    crate::delivery::schema_is_current(conn)
}

#[cfg(not(feature = "delivery"))]
fn delivery_schema_is_current(_conn: &Connection) -> Result<bool> {
    Ok(true)
}

/// Whether read-only APIs can safely and efficiently query this database.
pub fn schema_is_read_current(conn: &Connection) -> Result<bool> {
    schema_has_required_indexes(conn, REQUIRED_SCOPE_READ_INDEXES)
}

/// Whether the cache-only catalog can use its sort-free query plans.
pub fn schema_is_catalog_read_current(conn: &Connection) -> Result<bool> {
    schema_has_required_indexes(conn, REQUIRED_CATALOG_READ_INDEXES)
}

/// Whether bounded event pagination has both source-scoped and source-less indexes.
pub fn schema_is_event_read_current(conn: &Connection) -> Result<bool> {
    schema_has_required_indexes(conn, REQUIRED_EVENT_READ_INDEXES)
}

/// Whether delegation-topology reads can use their indexed parent and child
/// lookups over the v2 `session_relationships` shape.
pub fn schema_is_relationship_read_current(conn: &Connection) -> Result<bool> {
    schema_has_required_indexes(conn, REQUIRED_RELATIONSHIP_READ_INDEXES)
}

/// Whether bounded tool call and file edit pagination has its page indexes.
pub fn schema_is_evidence_read_current(conn: &Connection) -> Result<bool> {
    schema_has_required_indexes(conn, REQUIRED_EVIDENCE_READ_INDEXES)
}

fn schema_has_required_indexes(conn: &Connection, required_indexes: &[&str]) -> Result<bool> {
    let mut table = conn.prepare("SELECT 1 FROM sqlite_master WHERE name = ? LIMIT 1")?;
    for name in REQUIRED_TABLES
        .iter()
        .chain(REQUIRED_DELIVERY_TABLES.iter())
    {
        if !table.exists([*name])? {
            return Ok(false);
        }
    }
    for name in REQUIRED_TRIGGERS {
        if !table.exists([name])? {
            return Ok(false);
        }
    }
    let mut migration = conn.prepare("SELECT 1 FROM schema_migrations WHERE name = ? LIMIT 1")?;
    for name in REQUIRED_SCHEMA_MIGRATIONS
        .iter()
        .chain(REQUIRED_DELIVERY_MIGRATIONS.iter())
    {
        if !migration.exists([*name])? {
            return Ok(false);
        }
    }
    let columns: HashSet<String> = conn
        .prepare("SELECT name FROM pragma_table_info('history')")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    if !REQUIRED_HISTORY_COLUMNS
        .iter()
        .all(|needed| columns.contains(*needed))
    {
        return Ok(false);
    }
    let session_columns: HashSet<String> = conn
        .prepare("SELECT name FROM pragma_table_info('sessions')")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    if !REQUIRED_SESSIONS_COLUMNS
        .iter()
        .all(|needed| session_columns.contains(*needed))
    {
        return Ok(false);
    }
    let checkpoint_columns: HashSet<String> = conn
        .prepare("SELECT name FROM pragma_table_info('session_hydration_checkpoints')")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    if !REQUIRED_HYDRATION_CHECKPOINT_COLUMNS
        .iter()
        .all(|needed| checkpoint_columns.contains(*needed))
    {
        return Ok(false);
    }
    let presence_columns: HashSet<String> = conn
        .prepare("SELECT name FROM pragma_table_info('session_presences')")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    if !REQUIRED_SESSION_PRESENCE_COLUMNS
        .iter()
        .all(|needed| presence_columns.contains(*needed))
    {
        return Ok(false);
    }
    let marker_columns: HashSet<String> = conn
        .prepare("SELECT name FROM pragma_table_info('session_markers')")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    if !REQUIRED_SESSION_MARKER_COLUMNS
        .iter()
        .all(|(needed, _)| marker_columns.contains(*needed))
    {
        return Ok(false);
    }
    let event_columns: HashSet<String> = conn
        .prepare("SELECT name FROM pragma_table_info('session_events')")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    if !REQUIRED_SESSION_EVENT_COLUMNS
        .iter()
        .all(|(needed, _)| event_columns.contains(*needed))
    {
        return Ok(false);
    }
    let relationship_columns: HashSet<String> = conn
        .prepare("SELECT name FROM pragma_table_info('session_relationships')")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    if !REQUIRED_SESSION_RELATIONSHIP_COLUMNS
        .iter()
        .all(|needed| relationship_columns.contains(*needed))
    {
        return Ok(false);
    }
    let mut index =
        conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ? LIMIT 1")?;
    for name in required_indexes {
        if !index.exists([name])? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether any [`RETIRED_INDEXES`] entry still exists.
fn retired_indexes_present(conn: &Connection) -> Result<bool> {
    let mut index =
        conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ? LIMIT 1")?;
    for name in RETIRED_INDEXES {
        if index.exists([name])? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Open the database for reading only.
///
/// A read-only handle cannot acquire the write lock, so a query can neither
/// block the writer nor be blocked behind it -- WAL readers proceed against
/// their snapshot regardless of who is writing. Deliberately skips [`init_db`]:
/// applying the schema is itself a write, so routing every `search`/`recent`
/// through `open_db` made read commands contend for a lock they never needed.
///
/// Fails if the database does not exist yet; callers wanting create-on-demand
/// should fall back to [`open_db`].
pub fn open_db_readonly(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening {} read-only", path.display()))?;
    configure_busy_retry(&conn)?;
    Ok(conn)
}

pub fn init_db(conn: &Connection) -> Result<()> {
    // REPLACE must fire delete triggers so enabled delivery retains preimages.
    conn.pragma_update(None, "recursive_triggers", true)?;
    init_db_once(conn)
}

fn init_db_once(conn: &Connection) -> Result<()> {
    // A current database needs no write lock. Besides keeping ordinary opens
    // cheap, this lets sync reach its per-source contention handling when a
    // different writer already owns the ledger lock. A leftover retired index
    // counts as outstanding migration work: its DROP is a real write, so it
    // routes through the serialized pass below instead of this lock-free path.
    if schema_is_current(conn)? && !retired_indexes_present(conn)? {
        return Ok(());
    }
    enable_wal_for_migration(conn)?;
    // Serialize the complete DDL/backfill pass. Acquiring the write lock before
    // inspecting columns prevents concurrent first-open migrations from both
    // deciding to add the same column. The connection's bounded busy handler
    // covers ordinary writer overlap while preserving prompt lock diagnostics.
    let transaction = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    // Another first opener may have completed the migration while this
    // connection waited for the lock.
    if !schema_is_current(&transaction)? || retired_indexes_present(&transaction)? {
        init_db_locked(&transaction)?;
    }
    transaction.commit()?;
    Ok(())
}

/// One observed delegation edge, or one piece of related evidence whose child
/// has no provider-recorded identity.
///
/// Shared by the fresh-database path and the `session_relationships_v2`
/// rebuild below so the two can never describe different tables. The primary
/// key is `relationship_uid` rather than the child id: unlinked evidence has
/// no child id at all, and two sidecars of the same parent must not collapse
/// into one row.
const SESSION_RELATIONSHIPS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS session_relationships (
    source TEXT NOT NULL,
    parent_session_id TEXT NOT NULL,
    relationship_uid TEXT NOT NULL,
    child_session_id TEXT,
    relationship TEXT NOT NULL,
    identity_status TEXT NOT NULL CHECK(identity_status IN ('observed','unlinked')),
    child_agent_type TEXT,
    child_agent_name TEXT,
    child_model TEXT,
    spawn_depth INTEGER,
    evidence_kind TEXT NOT NULL,
    evidence_locator TEXT,
    evidence_ref TEXT,
    child_has_events INTEGER NOT NULL DEFAULT 0,
    spawned_at_ms INTEGER,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    origin_session_id TEXT,
    PRIMARY KEY (source, parent_session_id, relationship_uid)
);
"#;

/// One transcript's continuity evidence, keyed by the transcript itself.
///
/// Kept per file rather than per session because the evidence is a property
/// of the transcript: two files can carry the same in-log session id, and
/// that fact is exactly the fork signal. Persisting it is what makes
/// reconciliation incremental — a later hydration resolves an earlier file's
/// dangling parent uuid without re-reading that file.
const SESSION_CONTINUITY_EVIDENCE_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS session_continuity_evidence (
    source TEXT NOT NULL,
    locator TEXT NOT NULL,
    session_id TEXT NOT NULL,
    file_session_id TEXT,
    first_parent_uuid TEXT,
    first_ts_ms INTEGER,
    in_log_session_ids_json TEXT NOT NULL DEFAULT '[]',
    has_resume_marker INTEGER NOT NULL DEFAULT 0,
    resume_target TEXT,
    explicit_targets_json TEXT NOT NULL DEFAULT '{}',
    source_version TEXT,
    origin_session_id TEXT,
    -- Why this evidence has not produced its edges yet; NULL once resolved.
    pending_reason TEXT,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (source, locator)
);
"#;

fn init_db_locked(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA)?;
    // Before the trigger below, whose body deletes from these tables.
    conn.execute_batch(SESSION_RELATIONSHIPS_DDL)?;
    conn.execute_batch(SESSION_CONTINUITY_EVIDENCE_DDL)?;
    // A trigger created by an earlier release keeps its old body through every
    // `CREATE TRIGGER IF NOT EXISTS`, so a changed body has to drop the old
    // one first. Behind a marker, so it happens once rather than on every open.
    if !migration_applied(conn, "session_delete_continuity_reopen_v1")? {
        conn.execute_batch("DROP TRIGGER IF EXISTS delete_session_hydration_state;")?;
    }
    conn.execute_batch(
        r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    name TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS discovery_skips (
    source TEXT NOT NULL,
    locator TEXT NOT NULL,
    stamp TEXT NOT NULL,
    reason TEXT,
    updated_ms INTEGER,
    PRIMARY KEY (source, locator)
);
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT NOT NULL,
    source TEXT NOT NULL,
    cwd TEXT,
    git_branch TEXT,
    first_activity_ms INTEGER,
    last_activity_ms INTEGER,
    last_assistant_text TEXT,
    raw_path TEXT,
    parser_version INTEGER NOT NULL DEFAULT 1,
    first_prompt TEXT,
    models_json TEXT,
    originator TEXT,
    agent_version TEXT,
    repo_url TEXT,
    initial_commit TEXT,
    workspace_roots_json TEXT,
    source_stamp TEXT,
    discovery_state TEXT,
    project_key TEXT,
    project_key_method TEXT,
    PRIMARY KEY (session_id, source)
);
CREATE TABLE IF NOT EXISTS session_presences (
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    location TEXT NOT NULL CHECK(location IN ('local', 'remote')),
    raw_locator TEXT,
    source_stamp TEXT,
    discovery_state TEXT,
    PRIMARY KEY (source, session_id, location)
);
CREATE TABLE IF NOT EXISTS session_hydration_checkpoints (
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    location TEXT NOT NULL CHECK(location IN ('local', 'remote')),
    source_stamp TEXT,
    parser_version INTEGER NOT NULL,
    last_event_at_ms INTEGER,
    source_bytes INTEGER NOT NULL DEFAULT 0,
    records_parsed INTEGER NOT NULL DEFAULT 0,
    include_related INTEGER NOT NULL DEFAULT 1,
    last_tool_result_index INTEGER,
    updated_ms INTEGER NOT NULL,
    source_diagnostics_json TEXT,
    PRIMARY KEY (source, session_id, location)
);
CREATE TABLE IF NOT EXISTS session_identity_correlations (
    source TEXT NOT NULL,
    local_session_id TEXT NOT NULL,
    remote_session_id TEXT NOT NULL,
    relationship TEXT NOT NULL,
    evidence_kind TEXT NOT NULL,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (source, local_session_id, relationship)
);
CREATE TRIGGER IF NOT EXISTS delete_session_presences
AFTER DELETE ON sessions
BEGIN
    DELETE FROM session_presences
    WHERE source = OLD.source AND session_id = OLD.session_id;
END;
CREATE TRIGGER IF NOT EXISTS delete_session_hydration_state
AFTER DELETE ON sessions
BEGIN
    DELETE FROM session_hydration_checkpoints
    WHERE source = OLD.source AND session_id = OLD.session_id;
    -- Before the DELETE below, not after: those rows are the only record of
    -- which transcripts depend on this session, and reconciliation finds a
    -- dependent by reading the edge that points at it. Deleting the edges
    -- first would leave every dependent resolved, with nothing left to
    -- rediscover it by, so rehydrating this session would never rebuild the
    -- continuation it used to carry.
    UPDATE session_continuity_evidence
    SET pending_reason = 'unreconciled'
    WHERE source = OLD.source
      AND locator IN (
        SELECT evidence_locator FROM session_relationships
        WHERE source = OLD.source
          AND relationship IN ('continuation', 'fork', 'resume')
          AND evidence_locator IS NOT NULL
          AND (parent_session_id = OLD.session_id OR child_session_id = OLD.session_id)
      );
    DELETE FROM session_relationships
    WHERE source = OLD.source
      AND (parent_session_id = OLD.session_id OR child_session_id = OLD.session_id);
END;
CREATE TRIGGER IF NOT EXISTS delete_session_identity_correlations
AFTER DELETE ON sessions
BEGIN
    DELETE FROM session_identity_correlations
    WHERE source = OLD.source AND local_session_id = OLD.session_id;
END;
CREATE TRIGGER IF NOT EXISTS delete_session_markers
AFTER DELETE ON sessions
BEGIN
    DELETE FROM session_markers
    WHERE source = OLD.source AND session_id = OLD.session_id;
END;
INSERT OR IGNORE INTO schema_migrations (name) VALUES ('session_markers_v1');
CREATE TRIGGER IF NOT EXISTS delete_session_continuity_evidence
AFTER DELETE ON sessions
BEGIN
    DELETE FROM session_continuity_evidence
    WHERE source = OLD.source AND session_id = OLD.session_id;
END;
"#,
    )?;
    // `sessions` predates the shallow catalog. Databases created by an older
    // release keep the original nine columns, so every catalog field is added
    // here as an ignore-error ALTER; a fresh database gets the same shape from
    // the CREATE TABLE above and these become no-ops. Both paths converge.
    // The list is REQUIRED_SESSIONS_COLUMNS itself rather than a copy of it:
    // three declarations of the same nine names (CREATE TABLE, this loop, the
    // read-only guard) is two chances to drift, and every catalog column is
    // TEXT, so the guard list is the migration list.
    // The triggers above are current now, including the rebuilt one.
    conn.execute_batch(
        "INSERT OR IGNORE INTO schema_migrations (name) \
         VALUES ('session_delete_continuity_reopen_v1');",
    )?;
    migrate_session_relationships_v2(conn)?;
    migrate_tool_result_fidelity_v1(conn)?;
    // Additive: a v2 table predating continuity gains the one column the
    // continuity kinds need, and a fresh database already has it from the DDL
    // above, so both paths converge on the same shape.
    ensure_text_columns(conn, "session_relationships", &["origin_session_id"])?;
    ensure_text_columns(conn, "history", REQUIRED_HISTORY_COLUMNS)?;
    ensure_text_columns(conn, "sessions", REQUIRED_SESSIONS_COLUMNS)?;
    ensure_text_columns(conn, "session_presences", REQUIRED_SESSION_PRESENCE_COLUMNS)?;
    ensure_columns(conn, "session_markers", REQUIRED_SESSION_MARKER_COLUMNS)?;
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (name) VALUES ('session_markers_combined_v1')",
        [],
    )?;
    ensure_text_columns(
        conn,
        "session_hydration_checkpoints",
        REQUIRED_HYDRATION_CHECKPOINT_COLUMNS,
    )?;
    // One typed migration for every column `session_events` gained after its
    // DDL: the project-identity pair and the per-message raw facts. An
    // existing database gains them here, a fresh one already has them from the
    // CREATE TABLE, and both paths converge.
    ensure_columns(conn, "session_events", REQUIRED_SESSION_EVENT_COLUMNS)?;
    // The raw-facts marker records that the columns exist; the rows are
    // backfilled by re-parsing, which HYDRATION_PARSER_VERSION forces once and
    // which a plain `sync` does once per provider.
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (name) VALUES ('session_events_raw_facts_v1')",
        [],
    )?;
    // Canonical project identity (issue #175). The columns above are additive
    // and the index below is created unconditionally, so the marker records
    // only that this database has the shape -- it deliberately backfills no
    // data. Resolving a key means walking the filesystem from each distinct
    // `cwd`, which is not something a schema migration inside the open path
    // should do, and writing a path-fallback key for a checkout that does have
    // a remote would be a well-formed answer computed over nothing: it would
    // read as "resolved to a path" forever, and every consumer grouping by the
    // key would split that repository. NULL means "not resolved yet" honestly,
    // and [`refresh_project_identity`] fills it on the next sync or hydration.
    conn.execute_batch(
        "INSERT OR IGNORE INTO schema_migrations (name) VALUES ('session_project_key_v1');",
    )?;
    // Before the presence model every identity in the local evidence ledger
    // was local. Check the marker before attempting a write so an
    // already-current database remains cheap to check. The outer immediate
    // transaction makes concurrent first opens safe and crash-atomic.
    let presence_backfilled = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = 'session_presences_local_backfill_v1')",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !presence_backfilled {
        conn.execute_batch(
            r#"
INSERT OR IGNORE INTO session_presences
    (source, session_id, location, raw_locator, source_stamp, discovery_state)
SELECT source, session_id, 'local', raw_path, source_stamp, discovery_state
FROM sessions
WHERE session_id <> ''
  AND NOT EXISTS (
      SELECT 1 FROM schema_migrations
      WHERE name = 'session_presences_local_backfill_v1'
  );
INSERT OR IGNORE INTO session_presences (source, session_id, location)
SELECT source, session_id, 'local'
FROM (
    SELECT source, session_id FROM history WHERE session_id IS NOT NULL AND session_id <> ''
    UNION SELECT source, session_id FROM session_events WHERE session_id <> ''
    UNION SELECT source, session_id FROM tool_calls WHERE session_id <> ''
    UNION SELECT source, session_id FROM file_edits WHERE session_id <> ''
    UNION SELECT source, session_id FROM session_commit_links WHERE session_id <> ''
)
WHERE NOT EXISTS (
    SELECT 1 FROM schema_migrations
    WHERE name = 'session_presences_local_backfill_v1'
);
INSERT OR IGNORE INTO schema_migrations (name)
VALUES ('session_presences_local_backfill_v1');
"#,
        )?;
    }
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_history_hash ON history(prompt_hash)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_history_timestamp ON history(timestamp_ms DESC)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_history_session ON history(source, session_id)",
        [],
    )?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name)", [])?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_tags_session ON session_tags(source, session_id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_tags_tag ON session_tags(tag_id)",
        [],
    )?;
    // The write cost of the retired indexes showed up directly in
    // cold-discovery profiles; see RETIRED_INDEXES.
    for name in RETIRED_INDEXES {
        conn.execute(&format!("DROP INDEX IF EXISTS {name}"), [])?;
    }
    // The catalog's total order is (last_activity_ms DESC, source, session_id):
    // recency alone ties constantly, because every mtime-derived session in one
    // scan can share a timestamp, and a keyset paginator that cannot break
    // those ties silently drops rows between pages. These two indexes carry the
    // whole ORDER BY so the listing is still answered without a sort.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_recency ON sessions(last_activity_ms DESC, source, session_id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_source_recency ON sessions(source, last_activity_ms DESC, session_id)",
        [],
    )?;
    // Shallow discovery keys its "has this file changed?" lookup on the raw
    // path, because a transcript's session id is not known until it is read.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_raw_path ON sessions(source, raw_path)",
        [],
    )?;
    // Grouping and filtering by canonical project is the point of the column:
    // `(source, project_key, last_activity_ms)` answers "this project's
    // sessions, newest first" without a sort, and its `project_key IS NULL`
    // prefix is what [`refresh_project_identity`] scans for unresolved rows.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_project_key ON sessions(source, project_key, last_activity_ms DESC)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_presences_location ON session_presences(location, source, session_id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_presences_locator ON session_presences(location, source, raw_locator)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_relationships_parent ON session_relationships(source, parent_session_id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_relationships_child ON session_relationships(source, child_session_id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_continuity_parent_uuid ON session_continuity_evidence(source, first_parent_uuid)",
        [],
    )?;
    // Reconciliation and the pending-evidence diagnostic both read only the
    // rows still waiting on something, so the index is partial: a database
    // whose evidence has all resolved carries almost no index at all.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_continuity_pending ON session_continuity_evidence(source, session_id) WHERE pending_reason IS NOT NULL",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_events_session ON session_events(source, session_id)",
        [],
    )?;
    // Continuity resolves a transcript's first parent uuid to the session
    // holding the record with that uuid, which is a lookup by message id
    // across every session rather than within one.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_events_message ON session_events(source, message_id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_events_source_page ON session_events(source, session_id, ts_ms, id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_events_page ON session_events(session_id, ts_ms, id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_events_ts ON session_events(ts_ms DESC)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_events_role ON session_events(role)",
        [],
    )?;
    // Evidence pages always name one source and one session and continue on
    // `(ts_ms, id)` within the canonical `ts_ms IS NULL, ts_ms, id` order.
    // Both timestamps are nullable, so the "nulls last" half of that order is
    // carried by indexing the `ts_ms IS NULL` expression itself: an index over
    // the bare column still leaves SQLite sorting every page in a temp b-tree.
    // These also subsume the retired `(source, session_id)` indexes, which
    // were strict prefixes of them.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tool_calls_page_v2 ON tool_calls(source, session_id, (ts_ms IS NULL), ts_ms, id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_edits_page_v2 ON file_edits(source, session_id, (ts_ms IS NULL), ts_ms, id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_edits_path ON file_edits(file_path)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_markers_session ON session_markers(source, session_id, ts_ms)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_commit_links_session ON session_commit_links(source, session_id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_commit_links_commit ON session_commit_links(commit_sha)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_commit_links_repo ON session_commit_links(repo, branch)",
        [],
    )?;
    crate::observations::init_schema(conn)?;
    init_delivery_schema(conn)?;
    Ok(())
}

#[cfg(feature = "delivery")]
fn init_delivery_schema(conn: &Connection) -> Result<()> {
    crate::delivery::init_schema(conn)
}

#[cfg(not(feature = "delivery"))]
fn init_delivery_schema(_conn: &Connection) -> Result<()> {
    Ok(())
}

/// Whether a named migration has already run, on a database that may predate
/// the `schema_migrations` table itself.
fn migration_applied(conn: &Connection, name: &str) -> Result<bool> {
    let table: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master \
         WHERE type = 'table' AND name = 'schema_migrations')",
        [],
        |row| row.get(0),
    )?;
    if !table {
        return Ok(false);
    }
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = ?)",
        [name],
        |row| row.get(0),
    )?)
}

/// Rebuild `session_relationships` into its v2 shape once.
///
/// The v1 primary key `(source, parent_session_id, child_session_id)` required
/// a child id, so it could not record related evidence a provider does not
/// give a stable child identity, and it merged several such observations into
/// a single row. [`ensure_text_columns`] cannot change a primary key, so the
/// table is rebuilt behind a `schema_migrations` marker. The caller holds the
/// immediate migration transaction, which makes this crash-atomic and safe
/// against concurrent first opens.
///
/// `legacy_alter_table` keeps the rename from rewriting the
/// `delete_session_hydration_state` trigger to point at the temporary name;
/// its body is already correct for v2, where `child_session_id = OLD.session_id`
/// is NULL-safe false.
fn migrate_session_relationships_v2(conn: &Connection) -> Result<()> {
    let migrated: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = 'session_relationships_v2')",
        [],
        |row| row.get(0),
    )?;
    if migrated {
        return Ok(());
    }
    let uid_columns: i64 = conn.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('session_relationships') \
         WHERE name = 'relationship_uid'",
        [],
        |row| row.get(0),
    )?;
    if uid_columns == 0 {
        conn.pragma_update(None, "legacy_alter_table", true)?;
        let rebuild = (|| -> Result<()> {
            conn.execute_batch(
                "ALTER TABLE session_relationships RENAME TO session_relationships_v1;",
            )?;
            conn.execute_batch(SESSION_RELATIONSHIPS_DDL)?;
            // Every v1 row named a child, so all of them carry over as
            // observed identities. The evidence that established them was not
            // recorded at the time, which `legacy_hydration` states honestly.
            conn.execute_batch(
                r#"
INSERT OR IGNORE INTO session_relationships
    (source, parent_session_id, relationship_uid, child_session_id, relationship,
     identity_status, evidence_kind, child_has_events, created_ms, updated_ms)
SELECT r.source, r.parent_session_id, 'child:' || r.child_session_id, r.child_session_id,
       r.relationship, 'observed', 'legacy_hydration',
       EXISTS(SELECT 1 FROM session_events e
              WHERE e.source = r.source AND e.session_id = r.child_session_id),
       r.created_ms, r.created_ms
FROM session_relationships_v1 r;
DROP TABLE session_relationships_v1;
"#,
            )?;
            Ok(())
        })();
        // The rebuild's error is the one that explains a failed migration, so
        // it is propagated first and restoring the pragma cannot mask it.
        let restored = conn.pragma_update(None, "legacy_alter_table", false);
        rebuild?;
        restored?;
    }
    conn.execute_batch(
        "INSERT OR IGNORE INTO schema_migrations (name) VALUES ('session_relationships_v2');",
    )?;
    Ok(())
}

/// Per-tool-result fidelity columns, added in place for existing databases.
///
/// These are not all TEXT, so they cannot ride along on
/// [`ensure_text_columns`]: `payload_bytes`, `payload_truncated`,
/// `call_index`, `event_index` and the checkpoint's `last_tool_result_index`
/// are integers, and storing a byte count as TEXT would sort
/// lexicographically the moment anything ranked by it. The marker is part of
/// [`REQUIRED_SCHEMA_MIGRATIONS`], so a database written before this shape is
/// routed through the writable open instead of being read as current and
/// failing with `no such column`.
///
/// The caller holds the immediate migration transaction, which makes this
/// crash-atomic and safe against concurrent first opens.
const TOOL_RESULT_FIDELITY_COLUMNS: &[(&str, &str, &str)] = &[
    ("session_events", "tool_use_id", "TEXT"),
    ("session_events", "payload_bytes", "INTEGER"),
    ("session_events", "payload_truncated", "INTEGER"),
    ("session_events", "payload_hash", "TEXT"),
    ("session_events", "call_index", "INTEGER"),
    ("session_events", "event_index", "INTEGER"),
    ("session_events", "result_status", "TEXT"),
    ("session_events", "event_source", "TEXT"),
    ("session_events", "error_signal", "TEXT"),
    ("session_events", "subagent_session_id", "TEXT"),
    ("session_events", "agent_id", "TEXT"),
    (
        "session_hydration_checkpoints",
        "last_tool_result_index",
        "INTEGER",
    ),
];

fn migrate_tool_result_fidelity_v1(conn: &Connection) -> Result<()> {
    let migrated: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = 'session_events_tool_result_fidelity_v1')",
        [],
        |row| row.get(0),
    )?;
    if migrated {
        return Ok(());
    }
    for (table, column, kind) in TOOL_RESULT_FIDELITY_COLUMNS {
        let present: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?"),
            [column],
            |row| row.get(0),
        )?;
        if present == 0 {
            // `table`, `column` and `kind` come exclusively from the constant
            // list above, never from user input.
            conn.execute(
                &format!("ALTER TABLE {table} ADD COLUMN {column} {kind}"),
                [],
            )
            .with_context(|| format!("adding column {table}.{column}"))?;
        }
    }
    conn.execute_batch(
        "INSERT OR IGNORE INTO schema_migrations (name) VALUES ('session_events_tool_result_fidelity_v1');",
    )?;
    Ok(())
}

/// Add only columns that are actually absent.
///
/// The caller holds the migration write lock, so only genuinely missing
/// columns are altered and concurrent opens cannot race the same statement.
/// This avoids both guaranteed failing ALTERs and locale/version-sensitive
/// matching on SQLite error strings.
fn ensure_text_columns(conn: &Connection, table: &str, required: &[&str]) -> Result<()> {
    let typed: Vec<(&str, &str)> = required.iter().map(|column| (*column, "TEXT")).collect();
    ensure_columns(conn, table, &typed)
}

/// Add only columns that are actually absent, each with its own SQL type.
///
/// Same contract as [`ensure_text_columns`]; `session_events` needs it because
/// its raw-fact columns are a mix of TEXT and INTEGER.
fn ensure_columns(conn: &Connection, table: &str, required: &[(&str, &str)]) -> Result<()> {
    let existing: HashSet<String> = conn
        .prepare("SELECT name FROM pragma_table_info(?)")?
        .query_map([table], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;

    for (column, sql_type) in required
        .iter()
        .filter(|(column, _)| !existing.contains(*column))
    {
        // `table`, `column` and `sql_type` come exclusively from internal
        // constant lists, never from user input.
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {sql_type}"),
            [],
        )
        .with_context(|| format!("adding column {table}.{column}"))?;
    }
    Ok(())
}

pub fn prompt_hash(prompt: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prompt.as_bytes());
    format!("{:x}", hasher.finalize())[..16].to_string()
}

pub fn parse_claude(line: &str) -> Result<Option<HistoryEntry>> {
    let obj: serde_json::Value = serde_json::from_str(line)?;
    let display = obj
        .get("display")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if display.is_empty() {
        return Ok(None);
    }
    Ok(Some(HistoryEntry {
        id: 0,
        source: "claude".into(),
        session_id: obj
            .get("sessionId")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        project: obj
            .get("project")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        prompt: display.to_string(),
        prompt_hash: Some(prompt_hash(display)),
        timestamp_ms: obj.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0),
    }))
}

pub fn parse_codex(line: &str) -> Result<Option<HistoryEntry>> {
    let obj: serde_json::Value = serde_json::from_str(line)?;
    let text = obj
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if text.is_empty() {
        return Ok(None);
    }
    Ok(Some(HistoryEntry {
        id: 0,
        source: "codex".into(),
        session_id: obj
            .get("session_id")
            .or_else(|| obj.get("sessionId"))
            .and_then(|v| v.as_str())
            .map(str::to_string),
        project: None,
        prompt: text.to_string(),
        prompt_hash: Some(prompt_hash(text)),
        timestamp_ms: ((obj.get("ts").and_then(|v| v.as_f64()).unwrap_or(0.0)) * 1000.0) as i64,
    }))
}

pub fn parse_cursor_text(line: &str) -> Result<Option<String>> {
    let obj: serde_json::Value = serde_json::from_str(line)?;
    if obj.get("role").and_then(|v| v.as_str()) != Some("user") {
        return Ok(None);
    }
    let content = obj.pointer("/message/content");
    let mut text = String::new();
    if let Some(s) = content.and_then(|v| v.as_str()) {
        text = s.to_string();
    } else if let Some(items) = content.and_then(|v| v.as_array()) {
        for item in items {
            if item.get("type").and_then(|v| v.as_str()) == Some("text") {
                text = item
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                break;
            }
        }
    }
    let mut trimmed = text.trim().to_string();
    if trimmed.starts_with("<user_query>") && trimmed.ends_with("</user_query>") {
        trimmed = trimmed["<user_query>".len()..trimmed.len() - "</user_query>".len()]
            .trim()
            .to_string();
    }
    Ok((!trimmed.is_empty()).then_some(trimmed))
}

pub fn build_fts_query(terms: &[String], raw: bool) -> String {
    if raw {
        return terms.join(" ");
    }
    let mut positives = Vec::new();
    let mut negatives = Vec::new();
    for term in terms {
        if matches!(term.as_str(), "AND" | "OR" | "NOT")
            || term.ends_with('*')
            || (term.starts_with('"') && term.ends_with('"'))
        {
            return terms.join(" ");
        }
        if let Some(stripped) = term.strip_prefix('-') {
            if !stripped.is_empty() {
                negatives.push(stripped.to_string());
                continue;
            }
        }
        positives.push(term.clone());
    }
    if positives.is_empty() && !negatives.is_empty() {
        return "\"__ai_hist_no_positive_terms__\"".into();
    }
    let mut query = positives
        .iter()
        .map(|t| quote_fts_term(t))
        .collect::<Vec<_>>()
        .join(" ");
    if !negatives.is_empty() {
        query.push_str(" NOT ");
        query.push_str(
            &negatives
                .iter()
                .map(|t| quote_fts_term(t))
                .collect::<Vec<_>>()
                .join(" NOT "),
        );
    }
    query
}

fn quote_fts_term(term: &str) -> String {
    format!("\"{}\"", term.replace('"', "\"\""))
}

/// True when a SQLite error is FTS5 rejecting the MATCH expression itself.
///
/// FTS5 surfaces a malformed expression as a `SqliteFailure` whose message names
/// the fts5 parser, or -- when a bareword is parsed as a column reference -- as
/// `no such column: <term>`. Anything else (I/O, decoding, a genuine schema
/// mismatch) is a real failure and must not be relabelled.
fn is_fts5_syntax_error(error: &rusqlite::Error) -> bool {
    let rusqlite::Error::SqliteFailure(_, Some(message)) = error else {
        return false;
    };
    let m = message.to_ascii_lowercase();
    m.contains("fts5")
        || m.contains("malformed match")
        || m.contains("unterminated string")
        || m.starts_with("no such column")
}

/// Map an FTS5 expression error to actionable guidance, leaving every other
/// error untouched. Only applies in raw mode, where the caller supplied the
/// MATCH expression verbatim.
pub fn raw_fts_query_error(raw: bool, error: rusqlite::Error) -> anyhow::Error {
    if raw && is_fts5_syntax_error(&error) {
        anyhow::anyhow!(
            "Invalid raw FTS5 MATCH expression. Quote literal terms (for example, \"parity-check\") or remove --fts to use the default search."
        )
    } else {
        error.into()
    }
}

fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryEntry> {
    Ok(HistoryEntry {
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        project: row.get(3)?,
        prompt: row.get(4)?,
        prompt_hash: None,
        timestamp_ms: row.get(5)?,
    })
}

pub fn normalize_tag_name(name: &str) -> String {
    name.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn append_filters(sql: &mut String, params: &mut Vec<String>, filter: &QueryFilter, alias: &str) {
    if let Some(source) = &filter.source {
        sql.push_str(&format!(" AND {alias}.source = ?"));
        params.push(source.clone());
    }
    if let Some(project) = &filter.project {
        sql.push_str(&format!(" AND {alias}.project LIKE ?"));
        params.push(format!("%{project}%"));
    }
    if let Some(tag) = &filter.tag {
        sql.push_str(&format!(
            " AND EXISTS (SELECT 1 FROM session_tags st JOIN tags t ON t.id = st.tag_id WHERE st.source = {alias}.source AND st.session_id = {alias}.session_id AND t.name = ?)"
        ));
        params.push(normalize_tag_name(tag));
    }
    if let Some(before_ms) = filter.before_ms {
        sql.push_str(&format!(" AND {alias}.timestamp_ms < ?"));
        params.push(before_ms.to_string());
    }
}

fn append_scope_filter(sql: &mut String, scope: SessionScope, alias: &str) {
    match scope {
        SessionScope::Local => {
            // A row without a session identity cannot be remote-addressed and
            // remains part of the historical local surface. Identified rows
            // written before the presence model (or by an older binary) also
            // remain visible while they have no classification. Current remote
            // writers record presence before evidence, so a remote-only row
            // never relies on this compatibility fallback.
            sql.push_str(&format!(
                " AND ({alias}.session_id IS NULL OR EXISTS (SELECT 1 FROM session_presences sp_local WHERE sp_local.source = {alias}.source AND sp_local.session_id = {alias}.session_id AND sp_local.location = 'local') OR NOT EXISTS (SELECT 1 FROM session_presences sp_any WHERE sp_any.source = {alias}.source AND sp_any.session_id = {alias}.session_id))"
            ));
        }
        SessionScope::Remote => {
            sql.push_str(&format!(
                " AND EXISTS (SELECT 1 FROM session_presences sp_remote WHERE sp_remote.source = {alias}.source AND sp_remote.session_id = {alias}.session_id AND sp_remote.location = 'remote')"
            ));
        }
        SessionScope::All => {}
    }
}

/// Record that a provider session exists at one acquisition location.
///
/// The operation is idempotent and intentionally does not infer the opposite
/// location. A teleported session can therefore have both rows, while a cloud
/// catalog entry stays remote-only until local materialization is observed.
pub fn mark_session_presence(
    conn: &Connection,
    source: &str,
    session_id: &str,
    location: SessionLocation,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO session_presences (source, session_id, location) VALUES (?, ?, ?)",
        params![source, session_id, location.as_str()],
    )?;
    Ok(())
}

/// Return the observed locations for one canonical session identity.
///
/// An empty result is intentionally distinct from `local`: it means no
/// provenance row was recorded (for example, by an older writer).
pub fn session_locations(conn: &Connection, source: &str, session_id: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT location FROM session_presences \
         WHERE source = ? AND session_id = ? \
         ORDER BY CASE location WHEN 'local' THEN 0 ELSE 1 END",
    )?;
    let locations = stmt
        .query_map(params![source, session_id], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(locations)
}

/// Record a presence together with connector-specific cache metadata.
///
/// Locator and stamp live on the presence so a dual local/remote session can
/// retain independent change detection state. Canonical merged metadata stays
/// on `sessions` for the unified user-facing row.
#[allow(clippy::too_many_arguments)]
pub fn upsert_session_presence(
    conn: &Connection,
    source: &str,
    session_id: &str,
    location: SessionLocation,
    raw_locator: Option<&str>,
    source_stamp: Option<&str>,
    discovery_state: Option<&str>,
) -> Result<()> {
    // Discovery runs this once per freshly read session; the prepared
    // statement rides the connection's cache.
    conn.prepare_cached(
        "INSERT INTO session_presences \
         (source, session_id, location, raw_locator, source_stamp, discovery_state) \
         VALUES (?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, location) DO UPDATE SET \
         raw_locator = COALESCE(excluded.raw_locator, session_presences.raw_locator), \
         source_stamp = COALESCE(excluded.source_stamp, session_presences.source_stamp), \
         discovery_state = CASE \
             WHEN session_presences.discovery_state = 'full' THEN 'full' \
             ELSE COALESCE(excluded.discovery_state, session_presences.discovery_state) \
         END",
    )?
    .execute(params![
        source,
        session_id,
        location.as_str(),
        raw_locator,
        source_stamp,
        discovery_state
    ])?;
    Ok(())
}

/// One non-conversational fact a provider recorded about a session: a
/// compaction boundary, a system preamble, an opaque reasoning trace.
///
/// A marker is deliberately not a `session_events` row. `session_events` is
/// the transcript — things said, tools called, results returned — and forcing
/// "the context was compacted here" into it would mean choosing a `role` and a
/// `kind` that are both untrue. Markers carry no role and their `ts_ms` is
/// nullable, because a provider may record that something happened without
/// recording when.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionMarker {
    pub source: String,
    pub session_id: String,
    /// Stable per-session identity, so re-reading the same evidence upserts.
    pub marker_uid: String,
    pub kind: String,
    pub message_id: Option<String>,
    pub ts_ms: Option<i64>,
    pub text: Option<String>,
    /// The provider's own fields for this marker, verbatim.
    pub detail_json: Option<String>,
}

/// Record one marker, replacing what a previous read of the same evidence
/// wrote.
pub fn insert_session_marker(conn: &Connection, marker: &SessionMarker) -> Result<usize> {
    let changed = conn.execute(
        "INSERT INTO session_markers \
         (source, session_id, marker_uid, kind, message_id, ts_ms, text, detail_json) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, marker_uid) DO UPDATE SET \
         kind = excluded.kind, message_id = excluded.message_id, \
         ts_ms = excluded.ts_ms, text = excluded.text, detail_json = excluded.detail_json",
        params![
            marker.source,
            marker.session_id,
            marker.marker_uid,
            marker.kind,
            marker.message_id,
            marker.ts_ms,
            marker.text,
            marker.detail_json,
        ],
    )?;
    Ok(changed)
}

/// Every marker recorded for one session, oldest known time first. Markers
/// with no recorded time sort last, in insertion order.
pub fn session_markers(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Vec<SessionMarker>> {
    let mut stmt = conn.prepare(
        "SELECT source, session_id, marker_uid, kind, message_id, ts_ms, text, detail_json \
         FROM session_markers WHERE source = ? AND session_id = ? \
         ORDER BY ts_ms IS NULL, ts_ms, id",
    )?;
    let rows = stmt.query_map(params![source, session_id], |row| {
        Ok(SessionMarker {
            source: row.get(0)?,
            session_id: row.get(1)?,
            marker_uid: row.get(2)?,
            kind: row.get(3)?,
            message_id: row.get(4)?,
            ts_ms: row.get(5)?,
            text: row.get(6)?,
            detail_json: row.get(7)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn insert_history(conn: &Connection, entry: &HistoryEntry) -> Result<usize> {
    insert_history_at_location(conn, entry, SessionLocation::Local)
}

/// Insert one history row and record the acquisition location of its session.
/// Existing callers use [`insert_history`] for the local default; remote
/// connectors use this entry point so ingestion never manufactures a local
/// presence.
pub fn insert_history_at_location(
    conn: &Connection,
    entry: &HistoryEntry,
    location: SessionLocation,
) -> Result<usize> {
    if location == SessionLocation::Remote {
        anyhow::ensure!(
            entry
                .session_id
                .as_deref()
                .is_some_and(|session_id| !session_id.is_empty()),
            "remote history requires a non-empty session id"
        );
    }
    // Presence first preserves the important failure invariant without adding
    // a transaction per imported prompt: evidence can never commit before its
    // classification. A lone presence after a later insert failure is harmless
    // and a duplicate evidence insert still repairs a missing presence.
    if let Some(session_id) = entry.session_id.as_deref().filter(|id| !id.is_empty()) {
        mark_session_presence(conn, &entry.source, session_id, location)?;
    }
    let inserted = conn.execute(
        "INSERT OR IGNORE INTO history (source, session_id, project, prompt, prompt_hash, timestamp_ms) VALUES (?, ?, ?, ?, ?, ?)",
        params![entry.source, entry.session_id, entry.project, entry.prompt, entry.prompt_hash, entry.timestamp_ms],
    )?;
    Ok(inserted)
}

pub fn search(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
) -> Result<Vec<HistoryEntry>> {
    if terms.is_empty() {
        return recent(conn, filter);
    }
    let query = build_fts_query(terms, raw_fts);
    let mut sql = "SELECT h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms FROM history_fts f JOIN history h ON f.rowid = h.id WHERE history_fts MATCH ?".to_string();
    let mut params_vec = vec![query];
    append_filters(&mut sql, &mut params_vec, filter, "h");
    append_scope_filter(&mut sql, filter.scope, "h");
    sql.push_str(" ORDER BY h.timestamp_ms DESC LIMIT ?");
    params_vec.push(filter.limit.max(1).to_string());
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_vec), row_to_entry)
        .map_err(|error| raw_fts_query_error(raw_fts, error))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| raw_fts_query_error(raw_fts, error))
}

pub fn recent(conn: &Connection, filter: &QueryFilter) -> Result<Vec<HistoryEntry>> {
    let mut sql = "SELECT h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms FROM history h WHERE 1=1".to_string();
    let mut params_vec = Vec::new();
    append_filters(&mut sql, &mut params_vec, filter, "h");
    append_scope_filter(&mut sql, filter.scope, "h");
    sql.push_str(" ORDER BY h.timestamp_ms DESC LIMIT ?");
    params_vec.push(filter.limit.max(1).to_string());
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), row_to_entry)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn session(
    conn: &Connection,
    session_id: &str,
    source: Option<&str>,
    tag: Option<&str>,
) -> Result<Vec<HistoryEntry>> {
    let mut filter = QueryFilter {
        limit: 10_000,
        source: source.map(str::to_string),
        tag: tag.map(str::to_string),
        ..Default::default()
    };
    let mut sql = "SELECT h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms FROM history h WHERE h.session_id = ?".to_string();
    let mut params_vec = vec![session_id.to_string()];
    append_filters(&mut sql, &mut params_vec, &filter, "h");
    sql.push_str(" ORDER BY h.timestamp_ms ASC");
    filter.limit = 0;
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), row_to_entry)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionEvent {
    pub id: i64,
    pub source: String,
    pub session_id: String,
    /// Raw working directory as the provider recorded it. Machine-local.
    pub project: Option<String>,
    /// Canonical project identity, denormalized from the owning session so a
    /// consumer can group events without joining. `None` until the session's
    /// identity has been resolved (see [`refresh_project_identity`]).
    pub project_key: Option<String>,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub message_id: Option<String>,
    pub parent_id: Option<String>,
    pub ts_ms: i64,
    pub role: String,
    pub kind: String,
    pub text: Option<String>,
    pub model: Option<String>,
    pub token_json: Option<String>,
    /// The upstream inference provider the harness named, when it names one.
    pub provider: Option<String>,
    pub event_uid: String,
    /// Per-tool-result fidelity. Every field below is `None` on a row that is
    /// not a tool result, and on a tool-result row whose provider does not
    /// record that fact — never a stand-in value, because a fabricated zero
    /// byte count or a guessed status reads exactly like a measured one.
    pub tool_use_id: Option<String>,
    /// Raw UTF-8 byte length of the provider's result payload, measured
    /// before `text` is materialized.
    pub payload_bytes: Option<i64>,
    /// 1 when the harness had already truncated the payload it handed back.
    pub payload_truncated: Option<i64>,
    /// First 16 hex characters of the payload's sha256.
    pub payload_hash: Option<String>,
    /// n-th result recorded for this `tool_use_id`, from zero.
    pub call_index: Option<i64>,
    /// Position of this result in the transcript's tool-result order.
    pub event_index: Option<i64>,
    /// `running` / `completed` / `errored` / `cancelled` / `unknown`.
    pub result_status: Option<String>,
    /// `tool_result` / `subagent_notification` / `function_call_output`.
    pub event_source: Option<String>,
    /// Which provider signal set the error: `tool_result.is_error`,
    /// `exit_code`, `patch_apply`, `mcp_err`, or `subagent_status`.
    pub error_signal: Option<String>,
    /// Delegated child session this result reports on.
    pub subagent_session_id: Option<String>,
    /// Delegated child agent this result reports on.
    pub agent_id: Option<String>,
    /// Verbatim provider envelope facts; see [`REQUIRED_SESSION_EVENT_COLUMNS`].
    pub request_id: Option<String>,
    /// Why the turn ended, as the harness reported it.
    pub stop_reason: Option<String>,
    pub agent_version: Option<String>,
    pub is_sidechain: Option<i64>,
    pub is_meta: Option<i64>,
    pub turn_id: Option<String>,
}

/// Stable continuation for normalized session events.
///
/// Event timestamps are not unique, so `id` is part of the cursor. The query
/// order is `(ts_ms ASC, id ASC)` and a page never needs to materialize the
/// rest of a large transcript.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionEventCursor {
    pub ts_ms: i64,
    pub id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionEventPage {
    pub events: Vec<SessionEvent>,
    pub next_cursor: Option<SessionEventCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionToolCall {
    pub id: i64,
    pub source: String,
    pub session_id: String,
    pub message_id: Option<String>,
    pub tool_use_id: String,
    pub name: String,
    pub target: Option<String>,
    pub args_json: Option<String>,
    pub is_error: Option<i64>,
    pub ts_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionFileEdit {
    pub id: i64,
    pub source: String,
    pub session_id: String,
    pub message_id: Option<String>,
    pub tool_use_id: String,
    pub file_path: String,
    pub tool_name: Option<String>,
    pub lines_added: Option<i64>,
    pub lines_removed: Option<i64>,
    pub structured_patch_json: Option<String>,
    pub user_modified: Option<i64>,
    pub ts_ms: Option<i64>,
    pub git_branch: Option<String>,
    pub cwd: Option<String>,
}

/// Bump whenever the session evidence row shapes, ordering, or cursor
/// semantics require an SDK change.
///
/// 2: `session_events` rows carry per-message raw provider facts and
/// per-tool-result fidelity, and the user-turn page is available.
pub const SESSION_EVIDENCE_CONTRACT_VERSION: u32 = 2;

/// Stable continuation for tool calls and file edits.
///
/// Neither table requires a timestamp, and the canonical order places the
/// undated tail last, so a cursor that only carried an `i64` could not say
/// whether it stands before or inside that tail. `ts_ms: None` means "already
/// inside the undated tail, continue by id".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionEvidenceCursor {
    pub ts_ms: Option<i64>,
    pub id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionToolCallPage {
    pub tool_calls: Vec<SessionToolCall>,
    pub next_cursor: Option<SessionEvidenceCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionFileEditPage {
    pub file_edits: Vec<SessionFileEdit>,
    pub next_cursor: Option<SessionEvidenceCursor>,
}

/// Column list and row mapper for [`SessionEvent`], declared once.
///
/// The full listing and the paged listing must return identical rows; when
/// each spelled its own `SELECT` the two drifted the moment a column was
/// added, and the mismatch only surfaces as a positional `row.get` reading the
/// wrong field.
const SESSION_EVENT_COLUMNS: &str =
    "id, source, session_id, project, project_key, cwd, git_branch, message_id, parent_id, \
     ts_ms, role, kind, text, model, token_json, provider, event_uid, tool_use_id, payload_bytes, \
     payload_truncated, payload_hash, call_index, event_index, result_status, event_source, \
     error_signal, subagent_session_id, agent_id, request_id, stop_reason, \
     agent_version, is_sidechain, is_meta, turn_id";

fn row_to_session_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionEvent> {
    Ok(SessionEvent {
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        project: row.get(3)?,
        project_key: row.get(4)?,
        cwd: row.get(5)?,
        git_branch: row.get(6)?,
        message_id: row.get(7)?,
        parent_id: row.get(8)?,
        ts_ms: row.get(9)?,
        role: row.get(10)?,
        kind: row.get(11)?,
        text: row.get(12)?,
        model: row.get(13)?,
        token_json: row.get(14)?,
        provider: row.get(15)?,
        event_uid: row.get(16)?,
        tool_use_id: row.get(17)?,
        payload_bytes: row.get(18)?,
        payload_truncated: row.get(19)?,
        payload_hash: row.get(20)?,
        call_index: row.get(21)?,
        event_index: row.get(22)?,
        result_status: row.get(23)?,
        event_source: row.get(24)?,
        error_signal: row.get(25)?,
        subagent_session_id: row.get(26)?,
        agent_id: row.get(27)?,
        request_id: row.get(28)?,
        stop_reason: row.get(29)?,
        agent_version: row.get(30)?,
        is_sidechain: row.get(31)?,
        is_meta: row.get(32)?,
        turn_id: row.get(33)?,
    })
}

/// All normalized events for one session, oldest first. Rows sharing a
/// timestamp keep insertion order via the rowid tiebreaker.
pub fn session_events(
    conn: &Connection,
    session_id: &str,
    source: Option<&str>,
) -> Result<Vec<SessionEvent>> {
    let mut sql =
        format!("SELECT {SESSION_EVENT_COLUMNS} FROM session_events WHERE session_id = ?");
    let mut params_vec = vec![session_id.to_string()];
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    sql.push_str(" ORDER BY ts_ms IS NULL, ts_ms ASC, id ASC");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), row_to_session_event)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// One bounded page of normalized events for a session, oldest first.
pub fn session_events_page(
    conn: &Connection,
    session_id: &str,
    source: Option<&str>,
    limit: i64,
    after: Option<&SessionEventCursor>,
) -> Result<SessionEventPage> {
    let limit = limit.clamp(1, 1_000);
    let mut sql =
        format!("SELECT {SESSION_EVENT_COLUMNS} FROM session_events WHERE session_id = ?");
    let mut params_vec = vec![session_id.to_string()];
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    if let Some(cursor) = after {
        sql.push_str(" AND (ts_ms > ? OR (ts_ms = ? AND id > ?))");
        params_vec.push(cursor.ts_ms.to_string());
        params_vec.push(cursor.ts_ms.to_string());
        params_vec.push(cursor.id.to_string());
    }
    sql.push_str(" ORDER BY ts_ms ASC, id ASC LIMIT ?");
    params_vec.push((limit + 1).to_string());

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), row_to_session_event)?;
    let mut events = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let has_more = events.len() > limit as usize;
    if has_more {
        events.truncate(limit as usize);
    }
    let next_cursor = has_more && !events.is_empty();
    let next_cursor = next_cursor.then(|| {
        let last = events.last().expect("non-empty page");
        SessionEventCursor {
            ts_ms: last.ts_ms,
            id: last.id,
        }
    });
    Ok(SessionEventPage {
        events,
        next_cursor,
    })
}

pub fn session_tool_calls(
    conn: &Connection,
    session_id: &str,
    source: Option<&str>,
) -> Result<Vec<SessionToolCall>> {
    let mut sql = format!("SELECT {TOOL_CALL_COLUMNS} FROM tool_calls WHERE session_id = ?");
    let mut params_vec = vec![session_id.to_string()];
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    sql.push_str(" ORDER BY ts_ms IS NULL, ts_ms ASC, id ASC");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), row_to_tool_call)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn session_file_edits(
    conn: &Connection,
    session_id: &str,
    source: Option<&str>,
) -> Result<Vec<SessionFileEdit>> {
    let mut sql = format!("SELECT {FILE_EDIT_COLUMNS} FROM file_edits WHERE session_id = ?");
    let mut params_vec = vec![session_id.to_string()];
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    sql.push_str(" ORDER BY ts_ms IS NULL, ts_ms ASC, id ASC");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), row_to_file_edit)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

const TOOL_CALL_COLUMNS: &str = "id, source, session_id, message_id, tool_use_id, name, target, \
                                 args_json, is_error, ts_ms";
const FILE_EDIT_COLUMNS: &str =
    "id, source, session_id, message_id, tool_use_id, file_path, tool_name, lines_added, \
     lines_removed, structured_patch_json, user_modified, ts_ms, git_branch, cwd";

fn row_to_tool_call(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionToolCall> {
    Ok(SessionToolCall {
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        message_id: row.get(3)?,
        tool_use_id: row.get(4)?,
        name: row.get(5)?,
        target: row.get(6)?,
        args_json: row.get(7)?,
        is_error: row.get(8)?,
        ts_ms: row.get(9)?,
    })
}

fn row_to_file_edit(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionFileEdit> {
    Ok(SessionFileEdit {
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        message_id: row.get(3)?,
        tool_use_id: row.get(4)?,
        file_path: row.get(5)?,
        tool_name: row.get(6)?,
        lines_added: row.get(7)?,
        lines_removed: row.get(8)?,
        structured_patch_json: row.get(9)?,
        user_modified: row.get(10)?,
        ts_ms: row.get(11)?,
        git_branch: row.get(12)?,
        cwd: row.get(13)?,
    })
}

/// One page's SQL and bound parameters, in the canonical
/// `ts_ms IS NULL, ts_ms ASC, id ASC` order.
///
/// A dated cursor must still admit the whole undated tail, and an undated one
/// must never walk back into the dated head, so the two continuation cases are
/// different predicates rather than one comparison over a coalesced timestamp.
///
/// The undated continuation is also a different *shape*. Its rows are exactly
/// the ones the page index stores under `(ts_ms IS NULL) = 1`, so it names that
/// indexed expression itself -- SQLite does not infer it from the bare
/// `ts_ms IS NULL` term -- and, since the first two order keys are then
/// constant, orders on `id` alone. Written the obvious way instead, SQLite
/// cannot reach `id` in the index and re-sorts the whole remaining tail in a
/// temp b-tree on every page. Both spellings return the same rows in the same
/// order.
///
/// Both page readers and the query-plan test build their SQL here, so a plan
/// assertion cannot pass against a restated copy while the real query drifts.
fn evidence_page_query(
    columns: &str,
    table: &str,
    source: &str,
    session_id: &str,
    limit: i64,
    after: Option<&SessionEvidenceCursor>,
) -> (String, Vec<rusqlite::types::Value>) {
    let mut sql = format!("SELECT {columns} FROM {table} WHERE source = ? AND session_id = ?");
    let mut params: Vec<rusqlite::types::Value> =
        vec![source.to_string().into(), session_id.to_string().into()];
    match after.map(|cursor| (cursor.ts_ms, cursor.id)) {
        Some((Some(ts_ms), id)) => {
            sql.push_str(" AND (ts_ms IS NULL OR ts_ms > ? OR (ts_ms = ? AND id > ?))");
            params.push(ts_ms.into());
            params.push(ts_ms.into());
            params.push(id.into());
            sql.push_str(" ORDER BY ts_ms IS NULL, ts_ms ASC, id ASC");
        }
        Some((None, id)) => {
            sql.push_str(" AND ((ts_ms IS NULL) = 1 AND ts_ms IS NULL AND id > ?)");
            params.push(id.into());
            sql.push_str(" ORDER BY id ASC");
        }
        None => {
            sql.push_str(" ORDER BY ts_ms IS NULL, ts_ms ASC, id ASC");
        }
    }
    sql.push_str(" LIMIT ?");
    params.push((limit + 1).into());
    (sql, params)
}

/// One bounded page of recorded tool calls for one session, oldest first.
///
/// Both `source` and `session_id` are required: native session ids collide
/// across providers, and a page that filtered on the id alone would interleave
/// two unrelated sessions.
pub fn session_tool_calls_page(
    conn: &Connection,
    source: &str,
    session_id: &str,
    limit: i64,
    after: Option<&SessionEvidenceCursor>,
) -> Result<SessionToolCallPage> {
    let limit = limit.clamp(1, 1_000);
    let (sql, params_vec) = evidence_page_query(
        TOOL_CALL_COLUMNS,
        "tool_calls",
        source,
        session_id,
        limit,
        after,
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), row_to_tool_call)?;
    let mut tool_calls = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    // One extra row was requested, so `has_more` already implies a full page.
    let has_more = tool_calls.len() > limit as usize;
    if has_more {
        tool_calls.truncate(limit as usize);
    }
    let next_cursor = has_more.then(|| {
        let last = tool_calls.last().expect("non-empty page");
        SessionEvidenceCursor {
            ts_ms: last.ts_ms,
            id: last.id,
        }
    });
    Ok(SessionToolCallPage {
        tool_calls,
        next_cursor,
    })
}

/// One bounded page of recorded file edits for one session, oldest first.
///
/// Scoped to one `source` for the same reason as
/// [`session_tool_calls_page`].
pub fn session_file_edits_page(
    conn: &Connection,
    source: &str,
    session_id: &str,
    limit: i64,
    after: Option<&SessionEvidenceCursor>,
) -> Result<SessionFileEditPage> {
    let limit = limit.clamp(1, 1_000);
    let (sql, params_vec) = evidence_page_query(
        FILE_EDIT_COLUMNS,
        "file_edits",
        source,
        session_id,
        limit,
        after,
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), row_to_file_edit)?;
    let mut file_edits = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    // One extra row was requested, so `has_more` already implies a full page.
    let has_more = file_edits.len() > limit as usize;
    if has_more {
        file_edits.truncate(limit as usize);
    }
    let next_cursor = has_more.then(|| {
        let last = file_edits.last().expect("non-empty page");
        SessionEvidenceCursor {
            ts_ms: last.ts_ms,
            id: last.id,
        }
    });
    Ok(SessionFileEditPage {
        file_edits,
        next_cursor,
    })
}

/// One block inside a user turn: either the human's own text or one tool
/// result the harness attached to the same message.
///
/// `approx_tokens` is deliberately absent. Every estimate available here is a
/// bytes-per-token heuristic, and a heuristic served from a store that also
/// serves measured values is indistinguishable from a measurement at the call
/// site. Consumers that want a token count bring their own tokenizer and
/// apply it to `byte_len`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionUserTurnBlock {
    /// `text` or `tool_result`.
    pub kind: String,
    /// The call this block answers; `None` on a text block and on a tool
    /// result whose provider recorded no call id.
    pub tool_use_id: Option<String>,
    /// Raw payload bytes when the parser measured them, otherwise the UTF-8
    /// length of the stored text.
    pub byte_len: i64,
    /// 1 when the result is known to have failed, 0 when it is known to have
    /// succeeded, `None` when the provider has not said.
    pub is_error: Option<i64>,
}

/// One user-side message and the ordered blocks it carried.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionUserTurn {
    /// Row id of the turn's first event; the cursor's tiebreaker.
    pub id: i64,
    pub source: String,
    pub session_id: String,
    /// Provider message id the blocks share, when there is one.
    pub message_id: Option<String>,
    /// The nearest message recorded before this turn began, and the nearest
    /// one recorded after it, whichever side of the conversation each came
    /// from. `None` only when the session recorded no named message on that
    /// side at all. An event the provider left unnamed is passed over rather
    /// than nulling the field: it is not a message any consumer can reference,
    /// and the named message behind it is still the one that borders this
    /// turn. Blocks of this same turn are never reported as its neighbours.
    pub preceding_message_id: Option<String>,
    pub following_message_id: Option<String>,
    pub ts_ms: i64,
    pub blocks: Vec<SessionUserTurnBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionUserTurnPage {
    pub user_turns: Vec<SessionUserTurn>,
    pub next_cursor: Option<SessionEventCursor>,
}

/// Byte length of a stored `text` column, counted in bytes rather than
/// characters so it is comparable with a measured `payload_bytes`.
const USER_TURN_BYTE_LEN: &str = "COALESCE(payload_bytes, LENGTH(CAST(text AS BLOB)), 0)";

/// The grouping key for a user turn: a provider message id when the row has
/// one, otherwise the row's own identity, so an unattributed event becomes a
/// turn of its own instead of merging with every other unattributed event.
const USER_TURN_KEY: &str = "COALESCE(NULLIF(message_id, ''), 'event:' || id)";

/// Which rows are a human message, or a block that arrived inside one.
///
/// `role` is not the discriminator, and neither is "not one known exception".
/// Several kinds of row are stored with `role = 'tool_result'` because that is
/// what they are evidence of, while never having arrived on a user message:
/// a Claude subagent notification is a harness line about a delegated child,
/// and a Codex `function_call_output` is a standalone response item carrying
/// its own item id as `message_id`. Grouping by role turns each of them into
/// a "user turn" that is neither human text nor an in-message result -- a
/// Codex rollout with one prompt and three outputs reported four turns.
///
/// `event_source` records which of those a row is, so membership is asserted
/// rather than inferred: `tool_result` is the only source that means "a block
/// inside a message". This is an allowlist on purpose. The previous denylist
/// admitted anything it had not been told to exclude, which is how it let the
/// Codex outputs through after the notifications had already been caught;
/// naming what qualifies cannot fail that way.
///
/// A row indexed before `event_source` existed is null and therefore not
/// proven to belong to a user message, so it is left out rather than guessed
/// at. Those rows are transient: the one-time fidelity backfill pass populates
/// `event_source` for every transcript it can still read.
const USER_TURN_ROW_FILTER: &str =
    "(role = 'user' OR (role = 'tool_result' AND event_source = 'tool_result'))";

/// The message recorded next to a turn, on either side of it.
///
/// The neighbour is looked for from the turn's *first* block in `(ts_ms, id)`
/// order, and every row sharing that turn's key is excluded, so the turn's own
/// later blocks can never be reported as the message that follows it. The
/// search is over every event of the session, not only user-side ones: what
/// precedes a human turn is normally the assistant message it answers, which
/// is the whole point of asking. A row the provider left unnamed cannot be
/// the answer, so the search passes over it to the nearest row that can be --
/// reporting `NULL` there would say "no message borders this turn" about a
/// session that has one, and an empty id would be worse.
const USER_TURN_NEIGHBOUR_SELECT: &str = "SELECT NULLIF(message_id, '') FROM session_events \
     WHERE source = ? AND session_id = ? AND NULLIF(message_id, '') IS NOT NULL";

/// One bounded page of user turns for one session, oldest first.
///
/// Derived from `session_events` rather than a table of its own: the blocks
/// are exactly the user-side rows the parsers already wrote, and a second
/// copy of them would be a second thing to keep true. Computing it here
/// instead of in each consumer is what makes every consumer agree on where a
/// turn starts and how its bytes are counted.
///
/// A turn is the set of rows that arrived on one user message, selected by
/// [`USER_TURN_ROW_FILTER`] and grouped by provider message id. That is exactly
/// a Claude user message, whose text and tool-result blocks arrive together.
///
/// Codex has no such grouping: it records the human message and every function
/// output as separate response items, and an output is not part of the user's
/// message. A Codex turn is therefore the prompt alone. Its tool results are
/// still indexed, with all their fidelity facts -- they are read through the
/// event APIs, which is where a standalone result belongs. Anything else
/// stored as a tool result but not carried on a user message, such as a Claude
/// subagent notification, is excluded for the same reason.
pub fn session_user_turns_page(
    conn: &Connection,
    source: &str,
    session_id: &str,
    limit: i64,
    after: Option<&SessionEventCursor>,
) -> Result<SessionUserTurnPage> {
    let limit = limit.clamp(1, 1_000);
    // The turn headers and each turn's blocks are separate statements, and a
    // sync writing to this database between them would hand back a header
    // whose blocks had moved or vanished -- an empty `blocks` array on a turn
    // that has them, and a cursor that has already advanced past it. One
    // deferred read transaction puts every statement on one SQLite snapshot.
    // WAL readers do not block the writer, so this costs nothing but keeps
    // the page internally consistent.
    //
    // A caller that already holds a transaction supplies the snapshot itself;
    // opening a nested one would fail outright.
    let snapshot = conn
        .is_autocommit()
        .then(|| conn.unchecked_transaction())
        .transpose()?;
    let mut sql = format!(
        "SELECT {USER_TURN_KEY} AS turn_key, MIN(ts_ms) AS turn_ts, MIN(id) AS turn_id, \
         MIN(NULLIF(message_id, '')) AS turn_message_id \
         FROM session_events \
         WHERE source = ? AND session_id = ? AND {USER_TURN_ROW_FILTER} \
         GROUP BY turn_key"
    );
    let mut params_vec: Vec<rusqlite::types::Value> =
        vec![source.to_string().into(), session_id.to_string().into()];
    if let Some(cursor) = after {
        sql.push_str(" HAVING turn_ts > ? OR (turn_ts = ? AND turn_id > ?)");
        params_vec.push(cursor.ts_ms.into());
        params_vec.push(cursor.ts_ms.into());
        params_vec.push(cursor.id.into());
    }
    sql.push_str(" ORDER BY turn_ts ASC, turn_id ASC LIMIT ?");
    params_vec.push((limit + 1).into());

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;
    let mut turns = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let has_more = turns.len() > limit as usize;
    if has_more {
        turns.truncate(limit as usize);
    }

    let mut user_turns = Vec::with_capacity(turns.len());
    let mut block_stmt = conn.prepare(&format!(
        "SELECT role, kind, tool_use_id, {USER_TURN_BYTE_LEN}, result_status \
         FROM session_events \
         WHERE source = ? AND session_id = ? AND {USER_TURN_ROW_FILTER} \
           AND {USER_TURN_KEY} = ? \
         ORDER BY ts_ms ASC, id ASC"
    ))?;
    let mut preceding_stmt = conn.prepare(&format!(
        "{USER_TURN_NEIGHBOUR_SELECT} AND {USER_TURN_KEY} <> ? \
           AND (ts_ms < ? OR (ts_ms = ? AND id < ?)) \
         ORDER BY ts_ms DESC, id DESC LIMIT 1"
    ))?;
    let mut following_stmt = conn.prepare(&format!(
        "{USER_TURN_NEIGHBOUR_SELECT} AND {USER_TURN_KEY} <> ? \
           AND (ts_ms > ? OR (ts_ms = ? AND id > ?)) \
         ORDER BY ts_ms ASC, id ASC LIMIT 1"
    ))?;
    for (turn_key, ts_ms, id, message_id) in turns {
        let blocks = block_stmt
            .query_map(params![source, session_id, turn_key], |row| {
                let role: String = row.get(0)?;
                let kind: String = row.get(1)?;
                let tool_use_id: Option<String> = row.get(2)?;
                let byte_len: i64 = row.get(3)?;
                let result_status: Option<String> = row.get(4)?;
                let is_tool_result = role == "tool_result" || kind == "tool_result";
                Ok(SessionUserTurnBlock {
                    kind: if is_tool_result {
                        "tool_result"
                    } else {
                        "text"
                    }
                    .to_string(),
                    tool_use_id: is_tool_result.then_some(tool_use_id).flatten(),
                    byte_len,
                    // `is_error` has three states and the statuses have
                    // five, so the collapse has to keep "known" separate
                    // from "not yet known". `cancelled` is terminal and
                    // stated by the provider: it did not succeed, and
                    // reporting null would make it read exactly like a
                    // result still in flight. The status itself stays on the
                    // event for a consumer that needs to tell a cancellation
                    // from a failure.
                    is_error: match result_status.as_deref() {
                        Some("errored" | "cancelled") => Some(1),
                        Some("completed") => Some(0),
                        _ => None,
                    },
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let neighbour = |stmt: &mut rusqlite::Statement<'_>| -> Result<Option<String>> {
            Ok(stmt
                .query_row(
                    params![source, session_id, &turn_key, ts_ms, ts_ms, id],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()?
                .flatten())
        };
        let preceding_message_id = neighbour(&mut preceding_stmt)?;
        let following_message_id = neighbour(&mut following_stmt)?;
        user_turns.push(SessionUserTurn {
            id,
            source: source.to_string(),
            session_id: session_id.to_string(),
            message_id,
            preceding_message_id,
            following_message_id,
            ts_ms,
            blocks,
        });
    }

    let next_cursor = has_more.then(|| {
        let last = user_turns.last().expect("non-empty page");
        SessionEventCursor {
            ts_ms: last.ts_ms,
            id: last.id,
        }
    });
    // Read-only: nothing to commit, and an explicit rollback releases the
    // snapshot rather than leaving it to drop order.
    if let Some(snapshot) = snapshot {
        snapshot.rollback()?;
    }
    Ok(SessionUserTurnPage {
        user_turns,
        next_cursor,
    })
}

pub fn stats(conn: &Connection, tag: Option<&str>) -> Result<Stats> {
    stats_scoped(conn, tag, SessionScope::Local)
}

pub fn stats_scoped(conn: &Connection, tag: Option<&str>, scope: SessionScope) -> Result<Stats> {
    stats_scoped_by(conn, tag, scope, ProjectGrouping::default())
}

/// Statistics with an explicit `by_project` bucketing.
pub fn stats_scoped_by(
    conn: &Connection,
    tag: Option<&str>,
    scope: SessionScope,
    grouping: ProjectGrouping,
) -> Result<Stats> {
    let mut where_sql = " WHERE 1=1".to_string();
    let mut params_vec = Vec::new();
    append_scope_filter(&mut where_sql, scope, "h");
    if let Some(tag) = tag {
        where_sql.push_str(" AND EXISTS (SELECT 1 FROM session_tags st JOIN tags t ON t.id = st.tag_id WHERE st.source = h.source AND st.session_id = h.session_id AND t.name = ?)");
        params_vec.push(normalize_tag_name(tag));
    }
    let total = conn.query_row(
        &format!("SELECT COUNT(*) FROM history h{where_sql}"),
        rusqlite::params_from_iter(params_vec.clone()),
        |r| r.get(0),
    )?;
    let by_source = {
        let mut stmt = conn.prepare(&format!(
            "SELECT source, COUNT(*) FROM history h{where_sql} GROUP BY source ORDER BY source"
        ))?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params_vec.clone()), |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    let by_project = {
        let key = grouping.expression();
        let extra = format!("{where_sql} AND {key} IS NOT NULL");
        let mut stmt = conn.prepare(&format!(
            "SELECT {key}, COUNT(*) FROM history h {extra} \
             GROUP BY {key} ORDER BY COUNT(*) DESC LIMIT 10"
        ))?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params_vec.clone()), |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    let (first_timestamp_ms, last_timestamp_ms) = conn.query_row(
        &format!("SELECT MIN(timestamp_ms), MAX(timestamp_ms) FROM history h{where_sql}"),
        rusqlite::params_from_iter(params_vec),
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    Ok(Stats {
        total,
        by_source,
        by_project,
        grouping,
        first_timestamp_ms,
        last_timestamp_ms,
    })
}

// ---------------------------------------------------------------------------
// canonical project identity
// ---------------------------------------------------------------------------

/// Strength of a stored project identity, as a SQL expression over `alias`.
///
/// Upserts merge on this ranking so a later, weaker observation can never
/// overwrite a stronger one: a shallow rescan that cannot see the repository
/// any more must not replace a resolved remote key with the directory it was
/// run from. `0` is "nothing stored", which every real value outranks.
fn project_key_rank_sql(alias: &str) -> String {
    format!(
        "CASE WHEN {alias}.project_key IS NULL THEN 0 \
              WHEN {alias}.project_key_method = 'remote' THEN 3 \
              WHEN {alias}.project_key_method = 'inherited' THEN 2 \
              ELSE 1 END"
    )
}

/// The `ON CONFLICT ... DO UPDATE SET` fragment that merges project identity
/// into the `sessions` table. Shared by shallow discovery and full ingest so
/// the two cannot disagree about precedence.
pub(crate) fn project_key_merge_sql() -> String {
    let incoming = project_key_rank_sql("excluded");
    let stored = project_key_rank_sql("sessions");
    format!(
        "project_key = CASE WHEN ({incoming}) >= ({stored}) \
            THEN excluded.project_key ELSE sessions.project_key END, \
         project_key_method = CASE WHEN ({incoming}) >= ({stored}) \
            THEN excluded.project_key_method ELSE sessions.project_key_method END"
    )
}

/// How many times the inheritance pass is repeated.
///
/// One statement propagates one level, so a chain of delegations needs one
/// pass per level, and a bound below the deepest chain the ledger can hold
/// would silently leave the tail of that chain on a path key. It is therefore
/// tied to [`crate::relationships::MAX_TREE_MAX_DEPTH`], the depth the
/// relationship reader itself will traverse: anything the tree can show, this
/// can key.
///
/// The loop stops as soon as no inheritable row remains, so the bound is not
/// the normal exit. It is a termination guarantee for a pathological topology
/// — the ledger does not forbid a relationship cycle — and it is safe as a
/// bound because each pass converts rows to `inherited`, which the predicate
/// excludes, so the candidate set strictly shrinks.
const PROJECT_KEY_INHERITANCE_PASSES: usize = crate::relationships::MAX_TREE_MAX_DEPTH as usize;

/// Resolve, inherit and denormalize canonical project identity.
///
/// Three passes, in order, because each depends on the previous one:
///
/// 1. **Resolve.** Every distinct `cwd` belonging to a session with no key is
///    resolved once through the cached resolver, and its sessions are stamped.
///    This is the only pass that touches the filesystem, and it is bounded by
///    the number of *unresolved* sessions, so a steady-state run does nothing.
/// 2. **Inherit.** A delegated child whose own directory resolved to nothing
///    canonical adopts its parent's key with `method = 'inherited'`, which is
///    what makes a subagent's work roll up to the repository the parent was
///    working in (burn's #506 rollup requirement). Deliberately a post-pass
///    over `session_relationships` rather than something the parsers do: the
///    child is frequently ingested before its parent, so anything that ran
///    during parsing would depend on file ordering.
/// 3. **Denormalize.** `session_events.project_key` is brought in line with
///    its session, so grouping events by project needs no join.
/// 4. **Lend.** Events of a delegated thread the catalog does not hold at all
///    take the key of their nearest cataloged ancestor, stepping over any
///    uncataloged generations between.
///
/// Returns the number of rows written across the three passes. Idempotent: a
/// second call on an unchanged database returns 0.
pub fn refresh_project_identity(conn: &Connection) -> Result<usize> {
    let mut written = resolve_missing_project_keys(conn)?;
    written += inherit_project_keys(conn)?;
    written += denormalize_event_project_keys(conn)?;
    written += inherit_event_project_keys(conn)?;
    Ok(written)
}

/// Pass 1: stamp sessions with no key, and upgrade ones stuck on a borrowed
/// or provisional key.
///
/// A path key is not a settled answer, it is the absence of one. Only filling
/// NULLs would freeze it: `upsert_session` resolves from the working directory
/// alone, so a session whose checkout has since been deleted or moved gets a
/// path key, and nothing would ever revisit it — not this pass, which would
/// see a non-NULL key, and not shallow discovery, which skips a source whose
/// bytes have not changed. The repository's own `repo_url`, which the Codex
/// reader records, then sits in the same row unused while every worktree of
/// that repository files under a different key.
///
/// So a `path` row is reconsidered on every pass and replaced only by
/// something strictly better. An `inherited` row is reconsidered too, for the
/// mirror-image reason: the parent's repository is a stand-in, and a child
/// that later gains its own `origin` would otherwise wear the parent's key for
/// as long as its transcript stayed unchanged — which is forever.
///
/// Nothing here can downgrade a key. `remote` replaces `path` or `inherited`;
/// `path` replaces only `path` or NULL, never an inherited key, because a
/// borrowed repository says more about a session than the directory it
/// happened to run in.
fn resolve_missing_project_keys(conn: &Connection) -> Result<usize> {
    let pending: Vec<(Option<String>, Option<String>)> = conn
        .prepare(
            "SELECT DISTINCT cwd, repo_url FROM sessions \
             WHERE (project_key IS NULL \
                    OR project_key_method IN ('path', 'inherited')) \
               AND (cwd IS NOT NULL OR repo_url IS NOT NULL)",
        )?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    if pending.is_empty() {
        return Ok(0);
    }
    // The predicate the update would apply, asked as a question first.
    //
    // In the steady state every one of these rows resolves to the key it
    // already has: a `path` key is reconsidered on every pass precisely
    // because it might be upgradable, and almost always is not. Issuing the
    // UPDATE anyway takes the write lock to change nothing, on every sync, for
    // every path-keyed session -- and a `SQLITE_BUSY` on that no-op would fail
    // the whole refresh, taking inheritance and denormalization down with it.
    // The other two passes probe for the same reason.
    //
    // An `inherited` key is reconsidered too, but displaced only by a
    // `remote`: it is a borrowed key, so it outranks a path and loses to the
    // child's own repository. A child that later gains its own `origin` would
    // otherwise keep the parent's repository for as long as its transcript
    // stayed unchanged, which is forever.
    const UPGRADABLE: &str = "(project_key IS NULL \
             OR (project_key_method = 'path' \
                 AND (?2 = 'remote' OR project_key <> ?1)) \
             OR (project_key_method = 'inherited' AND ?2 = 'remote')) \
           AND cwd IS ?3 AND repo_url IS ?4";
    let mut needed = conn.prepare(&format!(
        "SELECT 1 FROM sessions WHERE {UPGRADABLE} LIMIT 1"
    ))?;
    let mut update = conn.prepare(&format!(
        "UPDATE sessions SET project_key = ?1, project_key_method = ?2 WHERE {UPGRADABLE}"
    ))?;
    let mut written = 0;
    for (cwd, repo_url) in pending {
        let Some((key, method)) =
            crate::project_identity::identity_for(cwd.as_deref(), repo_url.as_deref())
        else {
            continue;
        };
        let arguments = params![key, method.as_str(), cwd, repo_url];
        if !needed.exists(arguments)? {
            continue;
        }
        written += update.execute(arguments)?;
    }
    Ok(written)
}

/// Pass 2: a delegated child adopts its parent's canonical key.
///
/// Applies to a child with no key at all and to one that only resolved to its
/// own path: a path key is machine-local and says nothing about the project,
/// so the parent's repository is strictly better information. A child that
/// resolved its *own* remote keeps it — that is a stronger statement than the
/// parent's, and a subagent genuinely running in another checkout should not
/// be filed under the delegator's repository.
fn inherit_project_keys(conn: &Connection) -> Result<usize> {
    let parent = inheritable_parent_key_sql("sessions.source", "sessions.session_id");
    let inheritable = inheritable_session_sql();
    let sql = format!(
        "UPDATE sessions SET project_key = ({parent}), project_key_method = 'inherited' \
         WHERE {inheritable}"
    );
    let mut written = 0;
    for _ in 0..PROJECT_KEY_INHERITANCE_PASSES {
        // Each iteration settles one level of delegation: a row whose
        // parent already holds its final key reaches that key and matches the
        // predicate no more. For a tree the loop therefore exits on the first
        // iteration that writes nothing, one past the deepest chain.
        //
        // Since the predicate now also admits a row whose borrowed key has
        // gone stale, the candidate set no longer *strictly* shrinks, so for a
        // cycle — which the ledger does not forbid — the bound rather than the
        // predicate is what terminates the loop. That is what the bound has
        // always been for.
        //
        // Probing with a SELECT before issuing the UPDATE keeps the steady
        // state read-only: a sync with nothing to inherit must not take the
        // write lock, both because it is pure cost and because a database held
        // by another writer would otherwise turn a no-op into a failure.
        let mut changed = 0;
        if conn
            .prepare(&format!(
                "SELECT 1 FROM sessions WHERE {inheritable} LIMIT 1"
            ))?
            .exists([])?
        {
            changed += conn.execute(&sql, [])?;
        }
        // The statement above joins a child straight to its parent's catalog
        // row, so it cannot see past a generation the catalog does not hold —
        // and a delegated thread that delegates again is routine. The walk
        // covers exactly that gap, and has to run whether or not the statement
        // found anything: its probe asks a strictly narrower question than
        // this one does, so breaking on it would skip the walk entirely.
        changed += inherit_across_uncataloged_generations(conn)?;
        written += changed;
        if changed == 0 {
            break;
        }
    }
    Ok(written)
}

/// The part of pass 2 that cannot be one statement: a cataloged session whose
/// delegating ancestor is cataloged but whose *intermediate* generations are
/// not.
///
/// `r -> c -> g` with `c` evidence-only leaves `g` joined to nothing, so `g`
/// keeps a path key or none while the root it was delegated from carries the
/// repository. The events of such a session must not be keyed where the
/// session row itself is not, so this fills the row and lets the ordinary
/// denormalizing pass carry it down — and keeps it in step afterwards, when
/// the ancestor it borrowed from moves.
fn inherit_across_uncataloged_generations(conn: &Connection) -> Result<usize> {
    // Two kinds of row reach the walk, and everything else is the statement's:
    //
    //   - one already wearing a borrowed key, because re-lending has to be
    //     decided one row at a time against a visited set — a statement that
    //     re-lends rotates the keys of a delegation cycle forever;
    //   - one with no key, or only a path, whose direct parent has nothing to
    //     lend, because then the answer is further up than a join can see.
    //
    // A row the statement can settle is left to it rather than walked twice.
    let lendable_parent = inheritable_parent_key_sql("sessions.source", "sessions.session_id");
    let pending: Vec<(String, String)> = conn
        .prepare(&format!(
            "SELECT source, session_id FROM sessions \
             WHERE ((project_key_method = 'inherited' AND project_key IS NOT NULL) \
                    OR ((project_key IS NULL OR project_key_method = 'path') \
                        AND NOT EXISTS ({lendable_parent}))) \
               AND EXISTS (SELECT 1 FROM session_relationships r \
                     WHERE r.source = sessions.source \
                       AND r.child_session_id = sessions.session_id)"
        ))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    if pending.is_empty() {
        return Ok(0);
    }
    // The same ranking the rest of the module applies: a `remote` is never
    // touched, and a borrowed key is rewritten only when the ancestor now
    // holds a different one — so a settled database writes nothing and the
    // loop around this terminates.
    let mut update = conn.prepare(
        "UPDATE sessions SET project_key = ?3, project_key_method = 'inherited' \
         WHERE source = ?1 AND session_id = ?2 \
           AND (project_key IS NULL OR project_key_method = 'path' \
                OR (project_key_method = 'inherited' AND project_key IS NOT ?3))",
    )?;
    let mut written = 0;
    for (source, session_id) in pending {
        let Some(key) = lendable_ancestor_key_for(conn, &source, &session_id)? else {
            continue;
        };
        written += update.execute(params![source, session_id, key])?;
    }
    Ok(written)
}

/// The key pass 2 would adopt for a child: that of its earliest-recorded
/// delegating parent which has something better than a path of its own.
///
/// The child is named by two SQL expressions so that one statement serves both
/// callers — correlated against the `sessions` row pass 2 is updating, and
/// against bound parameters when discovery asks the same question about a
/// single row before the pass runs. The two must not drift: discovery streams
/// its answer to the caller while pass 2 writes the row, and a consumer
/// reading both would otherwise be told two different projects for one
/// session.
fn inheritable_parent_key_sql(source: &str, session_id: &str) -> String {
    format!(
        "SELECT p.project_key FROM session_relationships r \
         JOIN sessions p ON p.source = r.source AND p.session_id = r.parent_session_id \
         WHERE r.source = {source} AND r.child_session_id = {session_id} \
           AND p.project_key IS NOT NULL \
           AND p.project_key_method IN ('remote', 'inherited') \
         ORDER BY r.created_ms, r.parent_session_id LIMIT 1"
    )
}

/// The rows this one statement lends to: a delegated child whose own key is
/// absent or merely its own path, whose parent has something better.
///
/// Deliberately *not* rows that already wear a borrowed key. Re-lending one is
/// necessary — an ancestor can be promoted onto a `remote` of its own and
/// every stand-in beneath it has to move — but it cannot be done by a
/// set-at-a-time statement: the relationship ledger permits a cycle, and in an
/// inherited-only cycle `A -> B -> C -> A` a single UPDATE reads every parent
/// from the same pre-statement snapshot and *rotates* the three keys. The
/// iteration bound ends the call but not at a fixed point, so every later
/// refresh rotates them again and denormalizes the new arrangement onto every
/// event — permanent churn that each pass reports as work done.
///
/// Re-lending therefore belongs to the walk below, which carries a visited set
/// and so can tell a chain from a circle. What is left here strictly shrinks
/// its own candidate set each iteration — a row it rewrites becomes
/// `inherited` and stops matching — which is the loop's termination argument.
fn inheritable_session_sql() -> String {
    let parent = inheritable_parent_key_sql("sessions.source", "sessions.session_id");
    format!("(project_key IS NULL OR project_key_method = 'path') AND EXISTS ({parent})")
}

/// The key a session will hold once pass 1 and pass 2 have both run, computed
/// one row at a time from the filesystem and the relationship ledger.
///
/// This is what makes a *streamed* key trustworthy. Discovery decides a cached
/// row's key and hands it to its caller in the same breath, while the refresh
/// pass runs at the end of the pass over every row at once. Asking only what
/// the catalog holds right now is not enough to agree with it: the same pass
/// may promote this row's parent from a path to its own `remote` (pass 1) and
/// only then lend it down (pass 2) — after the child was already emitted.
///
/// So the answer is derived rather than read: the nearest ancestor, in the
/// order pass 2 selects parents, that has or is about to have a `remote` key
/// of its own. Bounded by [`PROJECT_KEY_INHERITANCE_PASSES`] and by a visited
/// set, because the ledger does not forbid a relationship cycle.
///
/// Returns `None` when nothing would be inherited, which is the common case:
/// most sessions have no parent at all, and the walk then costs one indexed
/// lookup that returns no rows.
pub(crate) fn inheritable_parent_project_key(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Option<String>> {
    lendable_ancestor_key_for(conn, source, session_id)
}

/// One candidate parent as the walk below reads it: its id, the key and
/// method it currently holds, and the two inputs pass 1 would resolve it from.
type ParentIdentityRow = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// The `remote` key this row will hold once pass 1 has run, if it will hold
/// one at all.
///
/// Two arms, both [`resolve_missing_project_keys`]'s `UPGRADABLE` predicate
/// read back out: a stored `remote` is never rewritten, and anything weaker —
/// `path`, `inherited`, or no key at all — is replaced by a `remote` the row
/// resolves for itself. Nothing else settles a row, so nothing else belongs
/// here; a key the row merely *borrowed* is the caller's business, because
/// pass 2 may replace that one too.
fn pass_one_remote_key(
    key: &Option<String>,
    method: Option<&str>,
    cwd: Option<String>,
    repo_url: Option<String>,
) -> Option<String> {
    if method == Some("remote") && key.is_some() {
        return key.clone();
    }
    match crate::project_identity::identity_for(cwd.as_deref(), repo_url.as_deref()) {
        Some((own, crate::project_identity::ProjectKeyMethod::Remote)) => Some(own),
        _ => None,
    }
}

/// The key the event's *own* session carries, when it has a catalog row.
const EVENT_SESSION_KEY_SQL: &str = "(SELECT s.project_key FROM sessions s \
     WHERE s.source = session_events.source \
       AND s.session_id = session_events.session_id)";

/// The method that key was arrived at by.
const EVENT_SESSION_METHOD_SQL: &str = "(SELECT s.project_key_method FROM sessions s \
     WHERE s.source = session_events.source \
       AND s.session_id = session_events.session_id)";

/// Rank a `(project_key, project_key_method)` pair the way the catalog does.
///
/// A method the row does not state ranks below everything. An event whose key
/// arrived through a plugin snapshot is exactly that case, and the evidence
/// contract already says such a key is never final — so it is improved by the
/// first pass that knows better, rather than being defended as if the emitter
/// had vouched for it.
fn event_key_rank_sql(key: &str, method: &str) -> String {
    format!(
        "CASE WHEN {key} IS NULL THEN 0 \
              WHEN {method} = 'remote' THEN 3 \
              WHEN {method} = 'inherited' THEN 2 \
              WHEN {method} = 'path' THEN 1 \
              ELSE 0 END"
    )
}

/// Pass 3: bring every event in line with its own session's key.
///
/// Ranked exactly as the catalog's merge is: nothing may lower a key's rank,
/// and an equal rank may only be rewritten when the key itself differs.
fn denormalize_event_project_keys(conn: &Connection) -> Result<usize> {
    let incoming = event_key_rank_sql(EVENT_SESSION_KEY_SQL, EVENT_SESSION_METHOD_SQL);
    let stored = event_key_rank_sql("project_key", "project_key_method");
    let stale = format!(
        "{EVENT_SESSION_KEY_SQL} IS NOT NULL \
           AND (({incoming}) > ({stored}) \
                OR (({incoming}) = ({stored}) \
                    AND (project_key IS NULL OR project_key <> {EVENT_SESSION_KEY_SQL})))"
    );
    // Probe first, for the same reason pass 2 does: the ingest path already
    // stamps each event as it inserts it, so this pass normally has nothing to
    // do and must not take the write lock to discover that.
    if !conn
        .prepare(&format!(
            "SELECT 1 FROM session_events WHERE {stale} LIMIT 1"
        ))?
        .exists([])?
    {
        return Ok(0);
    }
    Ok(conn.execute(
        &format!(
            "UPDATE session_events SET project_key = {EVENT_SESSION_KEY_SQL}, \
             project_key_method = {EVENT_SESSION_METHOD_SQL} WHERE {stale}"
        ),
        [],
    )?)
}

/// Events of a session the catalog does not hold rank as `inherited` once this
/// pass lends them an ancestor's key.
const EVENT_INHERITED_RANK: i64 = 2;

/// Pass 4: a delegated thread the catalog does not hold at all takes the key
/// of its nearest lendable ancestor.
///
/// This is where a rollup is won or lost. A delegated thread is evidence, not
/// a session — a Codex subagent rollout and a Claude sidechain produce
/// `session_events` under the child's id while the catalog holds only the root
/// a human actually ran — so without this those events are the one place in
/// the database with no project at all, which is exactly what burn's #506
/// rollup needs.
///
/// It has to *walk*. Joining the relationship parent straight to `sessions`
/// stops at the first generation the catalog does not hold, and a subagent
/// that delegates again is routine: for `root -> child -> grandchild` with
/// neither middle generation cataloged, the grandchild's events found no row
/// to join and kept a path key or none at all — the deeper the delegation, the
/// more certainly its work vanished from the project it belongs to. The walk
/// climbs past an ancestor stuck on its own `path` too, for the same reason
/// pass 2 does not lend one: a machine-local directory is the absence of an
/// answer, not an answer to pass down.
///
/// The loan is ranked like everything else here: it fills an event with no key
/// or a merely path-derived one, and replaces a borrowed key that has gone
/// stale, but never displaces a `remote` the thread resolved for itself. A
/// subagent that genuinely ran in another checkout stays filed where it ran.
fn inherit_event_project_keys(conn: &Connection) -> Result<usize> {
    let stored_rank = event_key_rank_sql("project_key", "project_key_method");
    // "No catalog row" is asked as exactly that, and not as "its key reads
    // NULL". The two are different sessions with different answers: a session
    // the catalog *does* hold is pass 2's business, and keying its events from
    // an ancestor while its own row stays null would put the row and its
    // events in two different projects — with pass 3 unable to reconcile them,
    // since it may not lower a key's rank. A cataloged session's events follow
    // its row, whatever that row ends up saying.
    let candidate = format!(
        "NOT EXISTS (SELECT 1 FROM sessions s \
             WHERE s.source = session_events.source \
               AND s.session_id = session_events.session_id) \
           AND ({stored_rank}) <= {EVENT_INHERITED_RANK} \
           AND EXISTS (SELECT 1 FROM session_relationships r \
                 WHERE r.source = session_events.source \
                   AND r.child_session_id = session_events.session_id)"
    );
    let pending: Vec<(String, String)> = conn
        .prepare(&format!(
            "SELECT DISTINCT source, session_id FROM session_events WHERE {candidate}"
        ))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    if pending.is_empty() {
        return Ok(0);
    }
    // The write repeats the candidate predicate — the rows were read without a
    // lock — and adds the one thing the walk decides: an equal-ranked key is
    // rewritten only when it actually differs, so a settled database writes
    // nothing.
    let mut update = conn.prepare(&format!(
        "UPDATE session_events SET project_key = ?3, project_key_method = 'inherited' \
         WHERE source = ?1 AND session_id = ?2 AND {candidate} \
           AND (({stored_rank}) < {EVENT_INHERITED_RANK} OR project_key <> ?3)"
    ))?;
    let mut written = 0;
    for (source, session_id) in pending {
        let Some(key) = lendable_ancestor_key_for(conn, &source, &session_id)? else {
            continue;
        };
        written += update.execute(params![source, session_id, key])?;
    }
    Ok(written)
}

/// [`lendable_ancestor_key`] for one session, with the visited set opened.
fn lendable_ancestor_key_for(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Option<String>> {
    let mut walk = AncestorWalk {
        visited: HashSet::new(),
        path: HashSet::new(),
    };
    walk.visited.insert(session_id.to_string());
    walk.path.insert(session_id.to_string());
    Ok(lendable_ancestor_key(conn, source, session_id, &mut walk, 0)?.0)
}

/// State carried through the ancestor walk.
struct AncestorWalk {
    /// Sessions already examined, so a shared ancestor is examined once and a
    /// circle is walked once rather than forever.
    visited: HashSet<String>,
    /// Sessions on the *current* path from the starting row. An edge back into
    /// this set closes a cycle; an edge into `visited` but not into this set is
    /// only a second route to the same ancestor, which is a diamond and
    /// perfectly ordinary. Conflating the two is what made a shared ancestor
    /// look like a loop.
    path: HashSet<String>,
}

/// The key of the nearest ancestor that has one worth lending, following the
/// same parent order, cycle protection and depth bound the rest of this module
/// uses.
///
/// **One walk, asked by two callers**, and that is the point of it: discovery
/// asks it for the key to *stream* before the refresh runs, and the refresh
/// asks it for the key to *store*. When they were two functions they drifted —
/// one of them inner-joined `sessions` and so could not see an evidence-only
/// parent, and a grandchild's row in the catalog and the same row in
/// `sessions discover --json` named different projects, with nothing anywhere
/// recording the disagreement.
///
/// Three kinds of ancestor are climbed *past* rather than stopping the search,
/// and all three are ordinary:
///
///   - one the catalog does not hold at all, because a delegated thread is
///     evidence rather than a session (hence the correlated subqueries: an
///     inner join drops exactly this case);
///   - one whose own key is only its `path`, which is the absence of an answer
///     rather than one — [`inheritable_parent_key_sql`] lends `remote` and
///     `inherited` and nothing else, and a walk that stopped at a path key
///     would label a machine-local directory `inherited` and call it a
///     project;
///   - one wearing a *borrowed* key, until its own ancestors have been asked:
///     promote an ancestor and every stand-in beneath it moves, so returning
///     the stand-in first answers with the key the refresh is about to
///     replace.
///
/// Returns the key, and whether a cycle was closed *below this point* — the
/// second is what tells the caller whether an ancestor's borrowed key is
/// grounded in anything or only in the row being keyed.
fn lendable_ancestor_key(
    conn: &Connection,
    source: &str,
    session_id: &str,
    walk: &mut AncestorWalk,
    depth: usize,
) -> Result<(Option<String>, bool)> {
    if depth >= PROJECT_KEY_INHERITANCE_PASSES {
        return Ok((None, false));
    }
    let parents: Vec<ParentIdentityRow> = conn
        .prepare(
            "SELECT r.parent_session_id, \
                    (SELECT p.project_key FROM sessions p \
                     WHERE p.source = r.source AND p.session_id = r.parent_session_id), \
                    (SELECT p.project_key_method FROM sessions p \
                     WHERE p.source = r.source AND p.session_id = r.parent_session_id), \
                    (SELECT p.cwd FROM sessions p \
                     WHERE p.source = r.source AND p.session_id = r.parent_session_id), \
                    (SELECT p.repo_url FROM sessions p \
                     WHERE p.source = r.source AND p.session_id = r.parent_session_id) \
             FROM session_relationships r \
             WHERE r.source = ?1 AND r.child_session_id = ?2 \
             ORDER BY r.created_ms, r.parent_session_id",
        )?
        .query_map(params![source, session_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut closed_a_cycle = false;
    for (parent_id, key, method, cwd, repo_url) in parents {
        if walk.path.contains(&parent_id) {
            // Back into the path we are standing on: this edge closes a cycle.
            closed_a_cycle = true;
            continue;
        }
        if !walk.visited.insert(parent_id.clone()) {
            // Seen on another branch and fully examined there. Nothing new is
            // reachable through it, and it is not a cycle.
            continue;
        }
        // What pass 1 will leave on this parent, by the rule pass 1 uses
        // rather than by what resolution alone would say. Pass 1 never
        // rewrites a `remote`, so a parent whose checkout now canonicalizes
        // differently keeps the key it has; answering with the fresh one would
        // stream a key the refresh then writes back over. Asked here rather
        // than only by the discovery caller because a refresh reaching this
        // walk has already run pass 1, so the two agree either way — and one
        // rule that is right in both places beats two that must be kept so.
        if let Some(settled) = pass_one_remote_key(&key, method.as_deref(), cwd, repo_url) {
            return Ok((Some(settled), closed_a_cycle));
        }
        walk.path.insert(parent_id.clone());
        let (found, cycle_below) =
            lendable_ancestor_key(conn, source, &parent_id, walk, depth + 1)?;
        walk.path.remove(&parent_id);
        if let Some(found) = found {
            return Ok((Some(found), closed_a_cycle || cycle_below));
        }
        closed_a_cycle |= cycle_below;
        // The borrowed key this ancestor is already wearing — unless the
        // search below *it* closed a cycle. A stand-in is only worth passing
        // on when it came from somewhere: in a ring of sessions that all
        // borrowed from each other, each one wears its key on the authority of
        // the next, and lending it round again just moves the ring. Declining
        // leaves those keys exactly as they are, which is the only stable
        // answer available.
        //
        // Scoped to this ancestor's own subtree, not to the whole walk: a
        // cycle somewhere else in the graph says nothing about whether *this*
        // key is grounded, and a walk that gave up after touching one left
        // every later parent's key on the floor.
        if !cycle_below && key.is_some() && method.as_deref() == Some("inherited") {
            return Ok((key, closed_a_cycle));
        }
    }
    Ok((None, closed_a_cycle))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn ensure_tag(conn: &Connection, name: &str, color: Option<&str>) -> Result<i64> {
    let normalized = normalize_tag_name(name);
    anyhow::ensure!(!normalized.is_empty(), "tag name cannot be empty");
    let now = now_ms();
    conn.execute(
        "INSERT INTO tags (name, display_name, color, created_ms, updated_ms) VALUES (?, ?, ?, ?, ?) ON CONFLICT(name) DO UPDATE SET display_name = excluded.display_name, color = COALESCE(excluded.color, tags.color), updated_ms = excluded.updated_ms",
        params![normalized, name.trim(), color, now, now],
    )?;
    Ok(
        conn.query_row("SELECT id FROM tags WHERE name = ?", [normalized], |r| {
            r.get(0)
        })?,
    )
}

pub fn matching_sessions(
    conn: &Connection,
    session_id: &str,
    source: Option<&str>,
) -> Result<Vec<TaggedSession>> {
    let mut sql = "SELECT source, session_id, MIN(project), COUNT(*), MAX(timestamp_ms) FROM history WHERE session_id = ?".to_string();
    let mut params_vec = vec![session_id.to_string()];
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    sql.push_str(" GROUP BY source, session_id ORDER BY source");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_vec), |r| {
            Ok(TaggedSession {
                source: r.get(0)?,
                session_id: r.get(1)?,
                project: r.get(2)?,
                entry_count: r.get(3)?,
                last_activity_ms: r.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn tag_session(
    conn: &Connection,
    session_id: &str,
    tag: &str,
    source: Option<&str>,
    color: Option<&str>,
) -> Result<Vec<TaggedSession>> {
    let sessions = matching_sessions(conn, session_id, source)?;
    if sessions.is_empty() {
        return Ok(sessions);
    }
    let tag_id = ensure_tag(conn, tag, color)?;
    let now = now_ms();
    for s in &sessions {
        conn.execute(
            "INSERT OR IGNORE INTO session_tags (source, session_id, tag_id, created_ms) VALUES (?, ?, ?, ?)",
            params![s.source, s.session_id, tag_id, now],
        )?;
    }
    Ok(sessions)
}

pub fn untag_session(
    conn: &Connection,
    session_id: &str,
    tag: &str,
    source: Option<&str>,
) -> Result<usize> {
    let sessions = matching_sessions(conn, session_id, source)?;
    let normalized = normalize_tag_name(tag);
    let mut removed = 0;
    for s in sessions {
        removed += conn.execute(
            "DELETE FROM session_tags WHERE source = ? AND session_id = ? AND tag_id IN (SELECT id FROM tags WHERE name = ?)",
            params![s.source, s.session_id, normalized],
        )?;
    }
    Ok(removed)
}

pub fn list_tags(conn: &Connection) -> Result<Vec<Tag>> {
    let mut stmt = conn.prepare(
        "SELECT t.name, t.display_name, t.color, COUNT(st.id), MIN(st.created_ms), MAX(st.created_ms) FROM tags t LEFT JOIN session_tags st ON st.tag_id = t.id GROUP BY t.id, t.name, t.display_name, t.color ORDER BY t.name",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(Tag {
                name: r.get(0)?,
                display_name: r.get(1)?,
                color: r.get(2)?,
                session_count: r.get(3)?,
                first_tagged_ms: r.get(4)?,
                last_tagged_ms: r.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn resume_command(entry: &HistoryEntry) -> Option<String> {
    let sid = entry.session_id.as_ref()?;
    match entry.source.as_str() {
        "claude" => Some(entry.project.as_ref().map_or_else(
            || format!("claude --resume {}", shell_quote(sid)),
            |p| {
                format!(
                    "cd {} && claude --resume {}",
                    shell_quote(p),
                    shell_quote(sid)
                )
            },
        )),
        "codex" => Some(format!("codex resume {}", shell_quote(sid))),
        "cursor" => Some(entry.project.as_ref().map_or_else(
            || format!("cursor-agent --resume={}", shell_quote(sid)),
            |p| {
                format!(
                    "cd {} && cursor-agent --resume={}",
                    shell_quote(p),
                    shell_quote(sid)
                )
            },
        )),
        "grok" => Some(entry.project.as_ref().map_or_else(
            || format!("grok resume {}", shell_quote(sid)),
            |p| format!("cd {} && grok resume {}", shell_quote(p), shell_quote(sid)),
        )),
        _ => None,
    }
}

pub fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

/// Whether the caller explicitly asked for the whole-database copy.
///
/// The copy is a per-sync `Connection::backup` of the provider's entire store
/// plus a provider-side index build on the copy; on a large `opencode.db` that
/// is hundreds of megabytes of I/O for evidence the bounded path reads with
/// session-keyed queries. It is now opt-in at *runtime* as well as at compile
/// time, so the default path does not copy even in a build that has the
/// feature — which is what `--all-features` gives CI.
#[cfg(feature = "opencode-backup")]
fn opencode_backup_requested() -> bool {
    std::env::var_os("AI_HIST_OPENCODE_BACKUP").is_some_and(|value| value == "1")
}

/// Index every OpenCode session the provider store holds.
///
/// The default path opens the live store read-only, pins one deferred read
/// transaction, and runs session-keyed queries against it. Nothing is copied,
/// no temporary database is created, and no provider DDL is issued, so the
/// cost is proportional to the history actually read rather than to the size
/// of the provider's database.
pub fn sync_opencode_db(conn: &Connection, opencode_db: &Path) -> Result<usize> {
    if !opencode_db.exists() {
        return Ok(0);
    }
    #[cfg(feature = "opencode-backup")]
    if opencode_backup_requested() {
        let tmp = tempfile::NamedTempFile::new()?.into_temp_path();
        let src_live = Connection::open_with_flags(
            opencode_db,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )
        .with_context(|| format!("opening {}", opencode_db.display()))?;
        src_live.busy_timeout(std::time::Duration::from_secs(5))?;
        src_live
            .backup(DatabaseName::Main, &tmp, None)
            .map_err(|source| SourceDatabaseError::new(opencode_db, source))?;
        let src = Connection::open(&tmp)?;
        src.execute_batch(
            "CREATE INDEX IF NOT EXISTS ai_hist_sync_part_session ON part(session_id);",
        )?;
        return sync_opencode_sessions_from_source(conn, &src, opencode_db);
    }
    let src = Connection::open_with_flags(
        opencode_db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening {}", opencode_db.display()))?;
    src.busy_timeout(std::time::Duration::from_secs(5))?;
    src.execute_batch("BEGIN")?;
    let result = sync_opencode_sessions_from_source(conn, &src, opencode_db);
    let _ = src.execute_batch("ROLLBACK");
    result
}

/// Index every OpenCode session in a legacy `storage/` JSON tree.
pub fn sync_opencode_storage_dir(conn: &Connection, storage_dir: &Path) -> Result<usize> {
    if !storage_dir.join("session").is_dir() {
        return Ok(0);
    }
    // One session's failure is that session's failure. The loader reports I/O
    // errors now, and propagating the first one would leave every healthy
    // session in the same tree unindexed for as long as the one path stays
    // broken -- the opposite of what making the read failure visible was for.
    // So each session is indexed or reported on its own, and the error at the
    // end names them all rather than the first.
    let mut inserted = 0;
    let listing = crate::ingest::opencode::list_json_tree_session_files(storage_dir);
    // A subtree that could not be walked is not a subtree with no sessions in
    // it. It joins the per-session failures rather than being dropped, so the
    // sessions under it are reported missing instead of silently absent.
    let mut failures: Vec<String> = listing
        .unreadable
        .iter()
        .map(|dir| format!("{}: {}", dir.path.display(), dir.error))
        .collect();
    for session_file in listing.sessions {
        let indexed =
            crate::ingest::opencode::load_from_json_tree(&session_file).and_then(|loaded| {
                match loaded {
                    Some(loaded) => crate::ingest::opencode::normalize(
                        conn,
                        &loaded,
                        &session_file.to_string_lossy(),
                    )
                    .map(|counts| counts.prompts),
                    None => Ok(0),
                }
            });
        match indexed {
            Ok(prompts) => inserted += prompts,
            Err(error) => failures.push(format!("{}: {error:#}", session_file.display())),
        }
    }
    if !failures.is_empty() {
        anyhow::bail!(
            "{} OpenCode path(s) under {} could not be read (the rest were indexed): {}",
            failures.len(),
            storage_dir.display(),
            failures.join("; ")
        );
    }
    Ok(inserted)
}

fn sync_opencode_sessions_from_source(
    conn: &Connection,
    src: &Connection,
    raw_path: &Path,
) -> Result<usize> {
    let raw_path = raw_path.to_string_lossy().into_owned();
    let mut inserted = 0;
    // One session's failure is that session's failure, the same way the legacy
    // tree's sweep treats it. The loader reports a row it cannot map rather
    // than calling the session absent, and taking that error out of the loop
    // with `?` would end the sweep at the first bad row -- every session after
    // it in the store unindexed for as long as that one row stays bad.
    let mut failures: Vec<String> = Vec::new();
    // Session-keyed queries are bounded only when the provider indexes the
    // column they seek on. Without that index each one scans `part`, and this
    // loop runs one per session -- quadratic on exactly the large stores the
    // bounded path exists to protect. Read the whole store once instead.
    match crate::ingest::opencode::sync_plan(src)? {
        crate::ingest::opencode::OpencodeSyncPlan::PerSession => {
            for session_id in crate::ingest::opencode::list_sqlite_session_ids(src)? {
                let indexed = crate::ingest::opencode::load_from_sqlite(src, &session_id).and_then(
                    |loaded| match loaded {
                        Some(loaded) => {
                            crate::ingest::opencode::normalize(conn, &loaded, &raw_path)
                                .map(|counts| counts.prompts)
                        }
                        None => Ok(0),
                    },
                );
                match indexed {
                    Ok(prompts) => inserted += prompts,
                    Err(error) => failures.push(format!("{session_id}: {error:#}")),
                }
            }
        }
        crate::ingest::opencode::OpencodeSyncPlan::SinglePass => {
            let load = crate::ingest::opencode::load_all_from_sqlite(src)?;
            failures.extend(
                load.failures
                    .iter()
                    .map(|failure| format!("{}: {}", failure.session_id, failure.error)),
            );
            for loaded in load.sessions {
                match crate::ingest::opencode::normalize(conn, &loaded, &raw_path) {
                    Ok(counts) => inserted += counts.prompts,
                    Err(error) => failures.push(format!("{}: {error:#}", loaded.session.id)),
                }
            }
        }
    }
    if !failures.is_empty() {
        anyhow::bail!(
            "{} OpenCode session(s) in {raw_path} could not be read (the rest were indexed): {}",
            failures.len(),
            failures.join("; ")
        );
    }
    Ok(inserted)
}

/// Ingest one OpenCode session with session-keyed queries against the live
/// source database. Unlike global sync this never enumerates the complete
/// provider store.
pub fn sync_opencode_session(
    conn: &Connection,
    opencode_db: &Path,
    session_id: &str,
) -> Result<usize> {
    let src = Connection::open_with_flags(
        opencode_db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening {}", opencode_db.display()))?;
    src.busy_timeout(std::time::Duration::from_secs(5))?;
    src.execute_batch("BEGIN")?;
    let result = sync_opencode_session_from_connection(conn, &src, session_id, opencode_db);
    let _ = src.execute_batch("ROLLBACK");
    result
}

/// Ingest one OpenCode session from a legacy `storage/` JSON tree.
pub fn sync_opencode_session_from_storage_dir(
    conn: &Connection,
    session_file: &Path,
    session_id: &str,
) -> Result<usize> {
    let Some(loaded) = crate::ingest::opencode::load_from_json_tree(session_file)? else {
        return Ok(0);
    };
    if loaded.session.id != session_id {
        anyhow::bail!(
            "OpenCode session file {} holds session '{}', not '{session_id}'",
            session_file.display(),
            loaded.session.id
        );
    }
    Ok(crate::ingest::opencode::normalize(conn, &loaded, &session_file.to_string_lossy())?.prompts)
}

fn sync_opencode_session_from_connection(
    conn: &Connection,
    src: &Connection,
    session_id: &str,
    raw_path: &Path,
) -> Result<usize> {
    let Some(loaded) = crate::ingest::opencode::load_from_sqlite(src, session_id)? else {
        return Ok(0);
    };
    Ok(crate::ingest::opencode::normalize(conn, &loaded, &raw_path.to_string_lossy())?.prompts)
}

pub fn export_json(conn: &Connection) -> Result<Vec<HistoryEntry>> {
    let mut stmt = conn.prepare("SELECT id, source, session_id, project, prompt, timestamp_ms FROM history ORDER BY timestamp_ms ASC")?;
    let rows = stmt
        .query_map([], row_to_entry)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn import_json(conn: &Connection, entries: &[HistoryEntry]) -> Result<usize> {
    let mut inserted = 0;
    for entry in entries {
        inserted += insert_history(conn, entry)?;
    }
    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_outdated_database_is_not_reported_as_schema_current() {
        let dir = std::env::temp_dir().join(format!("ai-hist-schema-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();

        // A database from before session_events existed: a read-only handle
        // skips init_db, so serving queries here would surface `no such table`
        // rather than migrating.
        let old_path = dir.join("old.db");
        let old = Connection::open(&old_path).unwrap();
        old.execute_batch(
            "CREATE TABLE history (id INTEGER PRIMARY KEY, source TEXT, prompt TEXT, timestamp_ms INTEGER);",
        )
        .unwrap();
        assert!(!schema_is_current(&old).unwrap());

        // A database opened through init_db has everything.
        let current = open_db(&dir.join("current.db")).unwrap();
        assert!(schema_is_current(&current).unwrap());

        fs::remove_dir_all(&dir).ok();
    }

    /// A database written by a release before OpenCode carried event-level
    /// evidence: `session_events` without `provider`/`stop_reason`, and no
    /// `session_markers` at all. The migration has to be additive — the rows
    /// already there survive it — and the read-only guard has to refuse the
    /// database until it has run, or a search would meet `no such column`.
    #[test]
    fn a_database_from_before_opencode_event_evidence_migrates_additively() {
        let dir = std::env::temp_dir().join(format!(
            "ai-hist-opencode-migration-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("old.db");

        {
            let old = Connection::open(&path).unwrap();
            old.execute_batch(
                "CREATE TABLE session_events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     source TEXT NOT NULL,
                     session_id TEXT NOT NULL,
                     project TEXT, cwd TEXT, git_branch TEXT,
                     message_id TEXT, parent_id TEXT,
                     ts_ms INTEGER NOT NULL,
                     role TEXT NOT NULL,
                     kind TEXT NOT NULL,
                     text TEXT, model TEXT, token_json TEXT,
                     event_uid TEXT NOT NULL,
                     UNIQUE(source, session_id, event_uid)
                 );
                 INSERT INTO session_events
                     (source, session_id, ts_ms, role, kind, text, event_uid)
                 VALUES ('opencode', 'ses_old', 1, 'user', 'text', 'kept', 'uid-1');",
            )
            .unwrap();
            assert!(
                !schema_is_current(&old).unwrap(),
                "an unmigrated database must not be served read-only"
            );
        }

        // Opening writably migrates.
        let migrated = open_db(&path).unwrap();
        assert!(schema_is_current(&migrated).unwrap());
        let (text, provider, stop_reason): (String, Option<String>, Option<String>) = migrated
            .query_row(
                "SELECT text, provider, stop_reason FROM session_events WHERE event_uid = 'uid-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(text, "kept", "the existing row survives the migration");
        assert_eq!(provider, None, "a backfilled column starts null");
        assert_eq!(stop_reason, None);
        assert!(session_markers(&migrated, "opencode", "ses_old")
            .unwrap()
            .is_empty());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_database_without_the_tool_result_columns_is_migrated_not_read_as_current() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pre-fidelity.db");
        {
            let conn = open_db(&path).unwrap();
            // Rewind the marker and drop the columns the way an older
            // release's database looks: the table is rebuilt without them,
            // because SQLite cannot drop a column a trigger references.
            conn.execute_batch(
                "DELETE FROM schema_migrations WHERE name = 'session_events_tool_result_fidelity_v1';
                 DROP TABLE session_events;
                 CREATE TABLE session_events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     source TEXT NOT NULL,
                     session_id TEXT NOT NULL,
                     project TEXT, cwd TEXT, git_branch TEXT,
                     message_id TEXT, parent_id TEXT,
                     ts_ms INTEGER NOT NULL,
                     role TEXT NOT NULL,
                     kind TEXT NOT NULL,
                     text TEXT, model TEXT, token_json TEXT,
                     event_uid TEXT NOT NULL,
                     UNIQUE(source, session_id, event_uid)
                 );
                 INSERT INTO session_events (source, session_id, ts_ms, role, kind, text, event_uid)
                 VALUES ('claude', 'legacy', 1, 'tool_result', 'tool_result', 'old output', 'e1');",
            )
            .unwrap();
            // A read-only handle skips init_db, so the guard has to say the
            // schema is not current or the next page read fails with
            // `no such column` instead of migrating.
            assert!(!schema_is_current(&conn).unwrap());
            assert!(!schema_is_event_read_current(&conn).unwrap());
        }

        let migrated = open_db(&path).unwrap();
        assert!(schema_is_current(&migrated).unwrap());
        let events = session_events(&migrated, "legacy", Some("claude")).unwrap();
        assert_eq!(events.len(), 1);
        // A row indexed before the columns existed keeps its text and reports
        // no measurement, rather than a fabricated zero.
        assert_eq!(events[0].text.as_deref(), Some("old output"));
        assert_eq!(events[0].payload_bytes, None);
        assert_eq!(events[0].event_source, None);
        let checkpoint_columns: i64 = migrated
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('session_hydration_checkpoints') \
                 WHERE name = 'last_tool_result_index'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(checkpoint_columns, 1);
    }

    /// Seed two user turns, each with one tool-result block, in a file
    /// database so a second connection can write to it concurrently.
    fn seed_two_user_turns(path: &std::path::Path) {
        let conn = open_db(path).unwrap();
        for (index, message) in ["m1", "m2"].iter().enumerate() {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, message_id, ts_ms, role, kind, text, event_uid, \
                  tool_use_id, payload_bytes, event_index, result_status, event_source) \
                 VALUES ('claude', 's1', ?, ?, 'tool_result', 'tool_result', 'out', ?, \
                  ?, 3, ?, 'completed', 'tool_result')",
                rusqlite::params![
                    message,
                    (index as i64) + 1,
                    format!("uid-{message}"),
                    format!("tu-{message}"),
                    index as i64,
                ],
            )
            .unwrap();
        }
    }

    #[test]
    fn a_terminal_result_status_is_never_reported_as_unknown() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // One user message carrying four results, one per status a provider
        // can record. They share a message_id so they are one turn.
        for (index, status) in ["errored", "cancelled", "completed", "running"]
            .iter()
            .enumerate()
        {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, message_id, ts_ms, role, kind, text, event_uid, \
                  tool_use_id, payload_bytes, event_index, result_status, event_source) \
                 VALUES ('claude', 's1', 'm1', ?, 'tool_result', 'tool_result', 'out', ?, \
                  ?, 3, ?, ?, 'tool_result')",
                rusqlite::params![
                    (index as i64) + 1,
                    format!("uid-{status}"),
                    format!("tu-{status}"),
                    index as i64,
                    status,
                ],
            )
            .unwrap();
        }

        let page = session_user_turns_page(&conn, "claude", "s1", 100, None).unwrap();
        assert_eq!(page.user_turns.len(), 1);
        let seen: Vec<(Option<&str>, Option<i64>)> = page.user_turns[0]
            .blocks
            .iter()
            .map(|block| (block.tool_use_id.as_deref(), block.is_error))
            .collect();
        assert_eq!(
            seen,
            vec![
                // Known to have failed.
                (Some("tu-errored"), Some(1)),
                // Terminal and known not to have succeeded. Reporting null
                // here would say "we do not know" about an outcome the
                // provider stated, and would read identically to a result
                // still in flight.
                (Some("tu-cancelled"), Some(1)),
                // Known to have succeeded.
                (Some("tu-completed"), Some(0)),
                // Genuinely undecided: still running.
                (Some("tu-running"), None),
            ],
        );

        // The status itself is not flattened away -- a consumer that needs to
        // tell a cancellation from a failure reads it from the event.
        let statuses: Vec<Option<String>> = session_events(&conn, "s1", Some("claude"))
            .unwrap()
            .into_iter()
            .map(|event| event.result_status)
            .collect();
        assert_eq!(
            statuses,
            vec![
                Some("errored".into()),
                Some("cancelled".into()),
                Some("completed".into()),
                Some("running".into()),
            ],
        );
    }

    #[test]
    fn a_user_turn_names_the_messages_either_side_of_it() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // One session, in recorded order: an assistant message, a user turn
        // whose text and tool result share a message id, an assistant
        // message with no id of its own, another assistant message, and a
        // second user turn at the end of the transcript.
        let insert = |message_id: &str, ts: i64, role: &str, uid: &str| {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, message_id, ts_ms, role, kind, text, event_uid, event_source) \
                 VALUES ('claude', 's1', ?, ?, ?, ?, 'body', ?, ?)",
                rusqlite::params![
                    message_id,
                    ts,
                    role,
                    if role == "tool_result" { "tool_result" } else { "text" },
                    uid,
                    if role == "tool_result" { Some("tool_result") } else { None },
                ],
            )
            .unwrap();
        };
        insert("a1", 1, "assistant", "e1");
        insert("m1", 2, "user", "e2");
        insert("m1", 3, "tool_result", "e3");
        // An event the provider left unnamed cannot be the answer to "which
        // message borders this turn", so the search passes over it to the one
        // that can. Nulling the field there would report that nothing borders
        // the turn, which is false of this session, and an empty id would be
        // worse still.
        insert("", 4, "assistant", "e4");
        insert("a2", 5, "assistant", "e5");
        insert("m2", 6, "user", "e6");

        let page = session_user_turns_page(&conn, "claude", "s1", 100, None).unwrap();
        let seen: Vec<(Option<&str>, Option<&str>, Option<&str>)> = page
            .user_turns
            .iter()
            .map(|turn| {
                (
                    turn.message_id.as_deref(),
                    turn.preceding_message_id.as_deref(),
                    turn.following_message_id.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            seen,
            vec![
                // The turn's own second block shares its message id, so it is
                // never reported as the message that follows the turn.
                (Some("m1"), Some("a1"), Some("a2")),
                // Nothing was recorded after the last turn.
                (Some("m2"), Some("a2"), None),
            ],
        );
        // A session that opens on the human's prompt has nothing before it.
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's2', 'm9', 1, 'user', 'text', 'body', 'f1'), \
                    ('claude', 's2', 'a9', 2, 'assistant', 'text', 'body', 'f2')",
            [],
        )
        .unwrap();
        let opening = session_user_turns_page(&conn, "claude", "s2", 100, None).unwrap();
        assert_eq!(opening.user_turns.len(), 1);
        assert_eq!(opening.user_turns[0].preceding_message_id, None);
        assert_eq!(
            opening.user_turns[0].following_message_id.as_deref(),
            Some("a9")
        );
    }

    #[test]
    fn a_user_turn_page_reads_one_snapshot_even_while_a_sync_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("turns.db");
        seed_two_user_turns(&path);

        let conn = open_db(&path).unwrap();
        // Commit a delete from a second connection in the middle of the page
        // read -- exactly between the header pass and the per-turn block
        // reads. WAL lets that writer through without blocking the reader, so
        // without a single snapshot the second turn comes back as a header
        // with an empty `blocks` array and a cursor already past it.
        // The write has to land in the window between the header pass and the
        // per-turn block reads, or it proves nothing: a delete that commits
        // before the first read is simply an earlier snapshot, which every
        // implementation reports consistently. So first measure what the
        // header pass costs in progress ticks, by running that exact query
        // standalone against the same data.
        let header_ticks = {
            let probe = open_db(&path).unwrap();
            let counter = std::rc::Rc::new(std::cell::Cell::new(0u32));
            let seen = std::rc::Rc::clone(&counter);
            // `progress_handler` wants a 'static closure; count through a
            // channel instead of borrowing the local.
            let (tx, rx) = std::sync::mpsc::channel();
            probe.progress_handler(
                1,
                Some(move || {
                    let _ = tx.send(());
                    false
                }),
            );
            let sql = format!(
                "SELECT {USER_TURN_KEY} AS turn_key, MIN(ts_ms) AS turn_ts, MIN(id) AS turn_id, \
                 MIN(NULLIF(message_id, '')) AS turn_message_id \
                 FROM session_events \
                 WHERE source = ? AND session_id = ? AND {USER_TURN_ROW_FILTER} \
                 GROUP BY turn_key ORDER BY turn_ts ASC, turn_id ASC LIMIT ?"
            );
            let mut stmt = probe.prepare(&sql).unwrap();
            let rows = stmt
                .query_map(rusqlite::params!["claude", "s1", 101], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(rows.len(), 2);
            drop(stmt);
            probe.progress_handler(0, None::<fn() -> bool>);
            while rx.try_recv().is_ok() {
                seen.set(seen.get() + 1);
            }
            counter.get()
        };
        assert!(header_ticks > 0, "the header pass must cost some ticks");

        // Delete on every tick from just past the header pass onwards (the
        // statement is idempotent). Without a shared snapshot the first of
        // those ticks falls between the header read and the block reads,
        // which is precisely the race.
        let writer_path = path.clone();
        let tick = std::sync::atomic::AtomicU32::new(0);
        conn.progress_handler(
            1,
            Some(move || {
                if tick.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= header_ticks {
                    let writer = Connection::open(&writer_path).unwrap();
                    writer
                        .execute("DELETE FROM session_events WHERE message_id = 'm2'", [])
                        .unwrap();
                }
                false
            }),
        );

        let page = session_user_turns_page(&conn, "claude", "s1", 100, None).unwrap();
        conn.progress_handler(0, None::<fn() -> bool>);

        // Which snapshot the page lands on depends on whether the delete beat
        // the first read, and either is correct: both turns, or only the one
        // that survived. What must never happen is the mixture -- a header
        // built from the old snapshot with blocks read from the new one, which
        // is a turn reporting itself as having no content at all, behind a
        // cursor that has already advanced past it.
        assert!(
            matches!(page.user_turns.len(), 1 | 2),
            "unexpected page size: {:?}",
            page.user_turns,
        );
        for turn in &page.user_turns {
            assert_eq!(
                turn.blocks.len(),
                1,
                "a header must keep the blocks it was built from: {turn:?}"
            );
        }

        // The delete really did land, so the assertion above is about the
        // snapshot and not about a write that never happened.
        let remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE message_id = 'm2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 0);
    }

    #[test]
    fn a_user_turn_page_uses_a_callers_transaction_instead_of_nesting_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("turns-nested.db");
        seed_two_user_turns(&path);

        let mut conn = open_db(&path).unwrap();
        // SQLite has no nested transactions, so opening one unconditionally
        // would turn every call from inside a caller's transaction into an
        // error. The caller's transaction is already the snapshot.
        let tx = conn.transaction().unwrap();
        let page = session_user_turns_page(&tx, "claude", "s1", 100, None).unwrap();
        assert_eq!(page.user_turns.len(), 2);
        tx.rollback().unwrap();

        // A plain call leaves the connection as it found it.
        assert!(conn.is_autocommit());
        let page = session_user_turns_page(&conn, "claude", "s1", 100, None).unwrap();
        assert_eq!(page.user_turns.len(), 2);
        assert!(
            conn.is_autocommit(),
            "the read snapshot must be released before returning"
        );
    }

    #[test]
    fn session_relationships_v1_database_migrates_to_v2() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1.db");
        let old = Connection::open(&path).unwrap();
        old.execute_batch(
            "CREATE TABLE session_relationships (
                 source TEXT NOT NULL,
                 parent_session_id TEXT NOT NULL,
                 child_session_id TEXT NOT NULL,
                 relationship TEXT NOT NULL,
                 created_ms INTEGER NOT NULL,
                 PRIMARY KEY (source, parent_session_id, child_session_id)
             );
             INSERT INTO session_relationships VALUES ('codex', 'root', 'child', 'delegated', 77);",
        )
        .unwrap();
        drop(old);

        let conn = open_db(&path).unwrap();
        let row: (String, String, String, i64, i64) = conn
            .query_row(
                "SELECT relationship_uid, identity_status, evidence_kind, child_has_events, created_ms \
                 FROM session_relationships WHERE child_session_id = 'child'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(row.0, "child:child");
        assert_eq!(row.1, "observed");
        assert_eq!(row.2, "legacy_hydration");
        assert_eq!(row.3, 0);
        assert_eq!(row.4, 77);
        let marker: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = 'session_relationships_v2')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(marker);
        // The cascade trigger survives the rebuild and still reaches the
        // rebuilt table rather than the renamed original.
        conn.execute(
            "INSERT INTO sessions (session_id, source) VALUES ('root', 'codex')",
            [],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM sessions WHERE session_id = 'root' AND source = 'codex'",
            [],
        )
        .unwrap();
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_relationships", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(remaining, 0);
    }

    #[test]
    fn unmigrated_relationship_schema_is_not_read_current() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stale.db");
        let current = open_db(&path).unwrap();
        assert!(schema_is_relationship_read_current(&current).unwrap());
        current
            .execute_batch(
                "DROP INDEX idx_session_relationships_child; \
                 DELETE FROM schema_migrations WHERE name = 'session_relationships_v2';",
            )
            .unwrap();
        assert!(!schema_is_relationship_read_current(&current).unwrap());
        assert!(!schema_is_current(&current).unwrap());
        drop(current);

        let migrated = open_db(&path).unwrap();
        assert!(schema_is_relationship_read_current(&migrated).unwrap());
    }

    #[test]
    fn a_read_only_handle_can_read_but_never_write() {
        let dir = std::env::temp_dir().join(format!("ai-hist-ro-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ro.db");
        let writer = open_db(&path).unwrap();
        insert_history(
            &writer,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s".into()),
                project: None,
                prompt: "hello".into(),
                prompt_hash: None,
                timestamp_ms: 1,
            },
        )
        .unwrap();

        let reader = open_db_readonly(&path).unwrap();
        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);

        // The point of the read-only handle: it cannot take the write lock, so
        // a read command can never contend with or block the single writer.
        let err = reader
            .execute_batch("CREATE TABLE nope (x)")
            .expect_err("a read-only handle must reject writes");
        assert!(
            err.to_string().contains("readonly"),
            "expected a readonly error, got: {err}"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn open_db_retries_a_transient_busy_writer() {
        let dir = std::env::temp_dir().join(format!("ai-hist-busy-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("busy.db");
        let conn = open_db(&path).unwrap();

        // A competing writer is retried rather than failing immediately. The
        // releaser runs after several backoff steps, exercising the handler
        // rather than succeeding on the first lock attempt.
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            conn.execute_batch("COMMIT").unwrap();
        });
        open_db(&path)
            .unwrap()
            .execute_batch("CREATE TABLE contended_probe (x)")
            .expect("writer should wait for the lock, not fail with SQLITE_BUSY");
        releaser.join().unwrap();

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn busy_retry_sequence_is_bounded() {
        let minimum_ms: u64 = (0..BUSY_RETRY_ATTEMPTS)
            .map(|attempt| busy_retry_backoff_ms(attempt).unwrap())
            .sum();
        assert!(
            (30_000..31_000).contains(&minimum_ms),
            "minimum retry grace changed unexpectedly: {minimum_ms}ms"
        );
        let maximum_ms: u64 = (0..BUSY_RETRY_ATTEMPTS)
            .map(|attempt| {
                let backoff = busy_retry_backoff_ms(attempt).unwrap();
                backoff + backoff / BUSY_RETRY_JITTER_DIVISOR
            })
            .sum();
        assert!(
            (33_000..34_000).contains(&maximum_ms),
            "maximum retry grace changed unexpectedly: {maximum_ms}ms"
        );
        assert!(busy_retry_backoff_ms(BUSY_RETRY_ATTEMPTS).is_none());
        assert!(!busy_retry_handler(BUSY_RETRY_ATTEMPTS));
        assert!(!busy_retry_handler(BUSY_RETRY_ATTEMPTS + 1));
    }

    #[test]
    fn parses_claude_and_codex() {
        assert_eq!(
            parse_claude(r#"{"display":" hello ","timestamp":7,"project":"/p","sessionId":"s"}"#)
                .unwrap()
                .unwrap()
                .prompt,
            "hello"
        );
        assert_eq!(
            parse_codex(r#"{"text":"fix","ts":2,"session_id":"c"}"#)
                .unwrap()
                .unwrap()
                .timestamp_ms,
            2000
        );
    }

    #[test]
    fn fts_query_preserves_established_semantics() {
        assert_eq!(
            build_fts_query(&["deploy".into(), "-relay".into()], false),
            "\"deploy\" NOT \"relay\""
        );
        assert_eq!(
            build_fts_query(&["parity-check".into()], false),
            "\"parity-check\""
        );
        assert_eq!(build_fts_query(&["foo*".into()], false), "foo*");
    }

    #[test]
    fn session_scope_defaults_to_local_and_uses_lowercase_wire_names() {
        assert_eq!(SessionScope::default(), SessionScope::Local);
        assert_eq!(
            serde_json::to_string(&SessionScope::Remote).unwrap(),
            "\"remote\""
        );
        assert_eq!(
            serde_json::from_str::<SessionScope>("\"all\"").unwrap(),
            SessionScope::All
        );
    }

    #[test]
    fn search_and_recent_preserve_unclassified_rows_as_local() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (session_id, timestamp_ms) in [("legacy", 1), ("local", 2), ("remote", 3), ("both", 4)]
        {
            insert_history(
                &conn,
                &HistoryEntry {
                    id: 0,
                    source: "claude".into(),
                    session_id: Some(session_id.into()),
                    project: None,
                    prompt: "scopeprobe".into(),
                    prompt_hash: None,
                    timestamp_ms,
                },
            )
            .unwrap();
        }
        mark_session_presence(&conn, "claude", "local", SessionLocation::Local).unwrap();
        conn.execute(
            "DELETE FROM session_presences WHERE source = 'claude' AND session_id = 'remote'",
            [],
        )
        .unwrap();
        mark_session_presence(&conn, "claude", "remote", SessionLocation::Remote).unwrap();
        mark_session_presence(&conn, "claude", "both", SessionLocation::Local).unwrap();
        mark_session_presence(&conn, "claude", "both", SessionLocation::Remote).unwrap();
        // Presence writes are idempotent.
        mark_session_presence(&conn, "claude", "both", SessionLocation::Remote).unwrap();
        conn.execute(
            "INSERT INTO history (source, session_id, prompt, timestamp_ms) VALUES ('claude', 'unclassified', 'scopeprobe', 5)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO history (source, session_id, prompt, timestamp_ms) VALUES ('claude', NULL, 'scopeprobe', 6)",
            [],
        )
        .unwrap();

        let ids = |rows: Vec<HistoryEntry>| {
            rows.into_iter()
                .map(|row| row.session_id.unwrap_or_else(|| "<none>".into()))
                .collect::<Vec<_>>()
        };
        for query in [false, true] {
            let run = |scope| {
                let filter = QueryFilter {
                    limit: 20,
                    scope,
                    ..Default::default()
                };
                if query {
                    search(&conn, &["scopeprobe".into()], false, &filter).unwrap()
                } else {
                    recent(&conn, &filter).unwrap()
                }
            };
            assert_eq!(
                ids(run(SessionScope::Local)),
                ["<none>", "unclassified", "both", "local", "legacy"]
            );
            assert_eq!(ids(run(SessionScope::Remote)), ["both", "remote"]);
            assert_eq!(
                ids(run(SessionScope::All)),
                [
                    "<none>",
                    "unclassified",
                    "both",
                    "remote",
                    "local",
                    "legacy"
                ]
            );
        }
        assert_eq!(
            stats_scoped(&conn, None, SessionScope::Local)
                .unwrap()
                .total,
            5
        );
        assert_eq!(
            stats_scoped(&conn, None, SessionScope::Remote)
                .unwrap()
                .total,
            2
        );
        assert_eq!(
            stats_scoped(&conn, None, SessionScope::All).unwrap().total,
            6
        );
        // Direct lookup is identity-based rather than acquisition-scoped.
        assert_eq!(
            session(&conn, "remote", Some("claude"), None)
                .unwrap()
                .len(),
            1
        );

        let presence_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_presences WHERE source = 'claude' AND session_id = 'both' AND location = 'remote'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(presence_count, 1);
    }

    #[test]
    fn history_insert_failure_cannot_leave_remote_evidence_unclassified() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER reject_history_insert BEFORE INSERT ON history
             WHEN NEW.session_id = 'reject-me'
             BEGIN SELECT RAISE(ABORT, 'injected failure'); END;",
        )
        .unwrap();
        let entry = HistoryEntry {
            id: 0,
            source: "codex".into(),
            session_id: Some("reject-me".into()),
            project: None,
            prompt: "must roll back".into(),
            prompt_hash: None,
            timestamp_ms: 1,
        };
        assert!(insert_history_at_location(&conn, &entry, SessionLocation::Remote).is_err());
        let presence_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_presences WHERE source = 'codex' AND session_id = 'reject-me'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(presence_count, 1);
        let history_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source = 'codex' AND session_id = 'reject-me'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(history_count, 0);
    }

    #[test]
    fn duplicate_history_insert_repairs_a_missing_presence() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let entry = HistoryEntry {
            id: 0,
            source: "codex".into(),
            session_id: Some("repair-me".into()),
            project: None,
            prompt: "already stored".into(),
            prompt_hash: None,
            timestamp_ms: 1,
        };
        assert_eq!(
            insert_history_at_location(&conn, &entry, SessionLocation::Remote).unwrap(),
            1
        );
        conn.execute(
            "DELETE FROM session_presences WHERE source = 'codex' AND session_id = 'repair-me'",
            [],
        )
        .unwrap();
        assert_eq!(
            insert_history_at_location(&conn, &entry, SessionLocation::Remote).unwrap(),
            0
        );
        let locations: Vec<String> = conn
            .prepare(
                "SELECT location FROM session_presences WHERE source = 'codex' AND session_id = 'repair-me'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(locations, ["remote"]);
    }

    #[test]
    fn remote_history_rejects_missing_or_empty_session_identity() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for session_id in [None, Some(String::new())] {
            let entry = HistoryEntry {
                id: 0,
                source: "codex".into(),
                session_id,
                project: None,
                prompt: "must have remote identity".into(),
                prompt_hash: None,
                timestamp_ms: 1,
            };
            let error = insert_history_at_location(&conn, &entry, SessionLocation::Remote)
                .expect_err("identity-less remote history must fail closed");
            assert!(error
                .to_string()
                .contains("remote history requires a non-empty session id"));
        }
        let history_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
            .unwrap();
        let presence_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_presences", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(history_count, 0);
        assert_eq!(presence_count, 0);
    }

    #[test]
    fn tags_and_filters_sessions() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/p".into()),
                prompt: "release auth".into(),
                prompt_hash: Some(prompt_hash("release auth")),
                timestamp_ms: 1,
            },
        )
        .unwrap();
        tag_session(&conn, "s1", "Release", Some("claude"), None).unwrap();
        let rows = search(
            &conn,
            &["auth".into()],
            false,
            &QueryFilter {
                tag: Some("release".into()),
                limit: 10,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(list_tags(&conn).unwrap()[0].name, "release");
        assert_eq!(
            untag_session(&conn, "s1", "release", Some("claude")).unwrap(),
            1
        );
    }

    #[test]
    fn empty_search_returns_recent_filtered_entries() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "grok".into(),
                session_id: Some("g1".into()),
                project: Some("/p".into()),
                prompt: "relayfile migration".into(),
                prompt_hash: Some(prompt_hash("relayfile migration")),
                timestamp_ms: 2,
            },
        )
        .unwrap();
        tag_session(&conn, "g1", "Relayfile Migration", Some("grok"), None).unwrap();
        let rows = search(
            &conn,
            &[],
            false,
            &QueryFilter {
                tag: Some("relayfile migration".into()),
                limit: 10,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source, "grok");
    }

    #[test]
    fn deserializes_legacy_history_entries_and_quotes_empty_args() {
        let entry: HistoryEntry = serde_json::from_str(
            r#"{"id":1,"source":"codex","prompt":"legacy export","timestamp_ms":42}"#,
        )
        .unwrap();
        assert_eq!(entry.session_id, None);
        assert_eq!(entry.project, None);
        assert_eq!(entry.prompt_hash, None);
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn opencode_sync_reads_committed_wal_rows() {
        let dir = tempfile::tempdir().unwrap();
        let opencode_path = dir.path().join("opencode.db");
        let src = Connection::open(&opencode_path).unwrap();
        src.execute_batch(
            r#"
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER);
            CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
            CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
            INSERT INTO session VALUES ('oc-wal', '/tmp/opencode', 1700000000000);
            INSERT INTO message VALUES ('msg-wal', 'oc-wal', 1700000001000, '{"role":"user"}');
            INSERT INTO part VALUES ('part-wal', 'msg-wal', 'oc-wal', 1700000002000, '{"type":"text","text":"wal opencode prompt"}');
            "#,
        )
        .unwrap();

        let live = Connection::open(&opencode_path).unwrap();
        let live_count: i64 = live
            .query_row("SELECT COUNT(*) FROM part", [], |r| r.get(0))
            .unwrap();
        assert_eq!(live_count, 1);

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        assert_eq!(sync_opencode_db(&conn, &opencode_path).unwrap(), 1);
        let prompt: String = conn
            .query_row(
                "SELECT prompt FROM history WHERE source = 'opencode'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(prompt, "wal opencode prompt");

        drop(src);
    }

    #[test]
    fn cursor_rows_unwrap_the_user_query_envelope_around_the_real_prompt() {
        // Cursor writes the prompt inside a <user_query> envelope; the stored
        // prompt is the text, not the markup.
        assert_eq!(
            parse_cursor_text(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<user_query>\n tag the release \n</user_query>"}]}}"#
            )
            .unwrap(),
            Some("tag the release".to_string())
        );
        // Older rows carry the prompt with no envelope at all.
        assert_eq!(
            parse_cursor_text(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"  tag the release  "}]}}"#
            )
            .unwrap(),
            Some("tag the release".to_string())
        );
        // Content is sometimes a bare string rather than a parts array.
        assert_eq!(
            parse_cursor_text(r#"{"role":"user","message":{"content":"tag the release"}}"#)
                .unwrap(),
            Some("tag the release".to_string())
        );
    }

    #[test]
    fn cursor_rows_without_a_user_prompt_are_skipped() {
        // Only user turns are history; the assistant's reply is not a prompt.
        assert_eq!(
            parse_cursor_text(
                r#"{"role":"assistant","message":{"content":[{"type":"text","text":"ok"}]}}"#
            )
            .unwrap(),
            None
        );
        // Whitespace-only text carries no prompt.
        assert_eq!(
            parse_cursor_text(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"   \n  "}]}}"#
            )
            .unwrap(),
            None
        );
        // An envelope with nothing inside it is not a prompt either.
        assert_eq!(
            parse_cursor_text(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<user_query></user_query>"}]}}"#
            )
            .unwrap(),
            None
        );
        // Non-text parts (images, tool payloads) contribute no prompt text.
        assert_eq!(
            parse_cursor_text(
                r#"{"role":"user","message":{"content":[{"type":"image","url":"file:///shot.png"}]}}"#
            )
            .unwrap(),
            None
        );
    }

    fn resume_entry(source: &str, session_id: Option<&str>, project: Option<&str>) -> HistoryEntry {
        HistoryEntry {
            id: 0,
            source: source.into(),
            session_id: session_id.map(str::to_string),
            project: project.map(str::to_string),
            prompt: "tag the release".into(),
            prompt_hash: None,
            timestamp_ms: 1,
        }
    }

    #[test]
    fn resume_commands_use_each_agents_own_cli_and_cd_into_the_project() {
        assert_eq!(
            resume_command(&resume_entry("claude", Some("s1"), Some("/tmp/p"))),
            Some("cd /tmp/p && claude --resume s1".to_string())
        );
        // Codex resumes by session id alone; it has no project argument.
        assert_eq!(
            resume_command(&resume_entry("codex", Some("s1"), Some("/tmp/p"))),
            Some("codex resume s1".to_string())
        );
        assert_eq!(
            resume_command(&resume_entry("cursor", Some("s1"), Some("/tmp/p"))),
            Some("cd /tmp/p && cursor-agent --resume=s1".to_string())
        );
        assert_eq!(
            resume_command(&resume_entry("grok", Some("s1"), Some("/tmp/p"))),
            Some("cd /tmp/p && grok resume s1".to_string())
        );
        // A project with shell metacharacters is quoted, not interpolated raw.
        assert_eq!(
            resume_command(&resume_entry("claude", Some("s1"), Some("/tmp/my proj"))),
            Some("cd '/tmp/my proj' && claude --resume s1".to_string())
        );
    }

    #[test]
    fn resume_drops_the_cd_without_a_project_and_is_absent_without_a_session() {
        assert_eq!(
            resume_command(&resume_entry("claude", Some("s1"), None)),
            Some("claude --resume s1".to_string())
        );
        assert_eq!(
            resume_command(&resume_entry("cursor", Some("s1"), None)),
            Some("cursor-agent --resume=s1".to_string())
        );
        // No session id means there is nothing to resume.
        assert_eq!(
            resume_command(&resume_entry("claude", None, Some("/tmp/p"))),
            None
        );
        // Sources with no resumable CLI (opencode, trajectory) offer nothing.
        assert_eq!(
            resume_command(&resume_entry("opencode", Some("s1"), Some("/tmp/p"))),
            None
        );
        assert_eq!(
            resume_command(&resume_entry("trajectory", Some("s1"), Some("/tmp/p"))),
            None
        );
    }

    fn add_event(
        conn: &Connection,
        source: &str,
        session_id: &str,
        ts_ms: i64,
        role: &str,
        text: &str,
        token_json: Option<&str>,
        event_uid: &str,
    ) {
        conn.execute(
            "INSERT INTO session_events (source, session_id, project, cwd, git_branch, message_id, \
             parent_id, ts_ms, role, kind, text, model, token_json, event_uid) \
             VALUES (?, ?, '/tmp/p', '/tmp/p', 'main', NULL, NULL, ?, ?, 'text', ?, 'claude-opus-5', ?, ?)",
            rusqlite::params![source, session_id, ts_ms, role, text, token_json, event_uid],
        )
        .unwrap();
    }

    #[test]
    fn session_events_read_back_oldest_first_and_scoped_to_one_source() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // Inserted out of order, and with a tie the ordering must break by
        // insertion order rather than at random.
        add_event(&conn, "claude", "s1", 300, "assistant", "third", None, "e3");
        add_event(&conn, "claude", "s1", 100, "user", "first", None, "e1");
        add_event(
            &conn,
            "claude",
            "s1",
            300,
            "user",
            "fourth",
            Some(r#"{"input_tokens":10,"output_tokens":20}"#),
            "e4",
        );
        add_event(&conn, "claude", "s1", 200, "user", "second", None, "e2");
        // A different agent reusing the same session id must not bleed in.
        add_event(&conn, "codex", "s1", 150, "user", "other agent", None, "e5");

        let all = session_events(&conn, "s1", None).unwrap();
        assert_eq!(
            all.iter()
                .map(|e| e.text.as_deref().unwrap())
                .collect::<Vec<_>>(),
            vec!["first", "other agent", "second", "third", "fourth"]
        );

        let claude_only = session_events(&conn, "s1", Some("claude")).unwrap();
        assert_eq!(
            claude_only
                .iter()
                .map(|e| e.text.as_deref().unwrap())
                .collect::<Vec<_>>(),
            vec!["first", "second", "third", "fourth"]
        );
        assert_eq!(claude_only[0].role, "user");
        assert_eq!(claude_only[0].project.as_deref(), Some("/tmp/p"));
        assert_eq!(claude_only[0].git_branch.as_deref(), Some("main"));
        assert_eq!(claude_only[0].model.as_deref(), Some("claude-opus-5"));
        assert_eq!(claude_only[0].event_uid, "e1");

        // Token usage round-trips as parseable JSON, not an opaque blob.
        let usage: serde_json::Value =
            serde_json::from_str(claude_only[3].token_json.as_deref().unwrap()).unwrap();
        assert_eq!(usage["input_tokens"], 10);
        assert_eq!(usage["output_tokens"], 20);

        // An unknown session reads back empty rather than erroring.
        assert!(session_events(&conn, "nope", None).unwrap().is_empty());
    }

    #[test]
    fn tool_calls_and_file_edits_read_back_oldest_first_and_scoped_to_one_source() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute_batch(
            r#"
            INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name, target, args_json, is_error, ts_ms)
            VALUES ('claude', 's1', 'm2', 'tu2', 'Bash', 'cargo test', '{"command":"cargo test"}', 1, 200),
                   ('claude', 's1', 'm1', 'tu1', 'Read', '/tmp/p/lib.rs', '{"file_path":"/tmp/p/lib.rs"}', 0, 100),
                   ('codex', 's1', 'm9', 'tu9', 'Shell', 'ls', '{}', 0, 150);
            INSERT INTO file_edits (source, session_id, message_id, tool_use_id, file_path, tool_name, lines_added, lines_removed, user_modified, ts_ms)
            VALUES ('claude', 's1', 'm3', 'tu3', '/tmp/p/b.rs', 'Edit', 2, 1, 0, 300),
                   ('claude', 's1', 'm1', 'tu1', '/tmp/p/a.rs', 'Write', 10, 0, 1, 100),
                   ('codex', 's1', 'm9', 'tu9', '/tmp/p/z.rs', 'apply_patch', 1, 1, 0, 150);
            "#,
        )
        .unwrap();

        let calls = session_tool_calls(&conn, "s1", Some("claude")).unwrap();
        assert_eq!(
            calls.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["Read", "Bash"]
        );
        assert_eq!(calls[0].tool_use_id, "tu1");
        assert_eq!(calls[0].target.as_deref(), Some("/tmp/p/lib.rs"));
        assert_eq!(calls[0].is_error, Some(0));
        assert_eq!(calls[1].is_error, Some(1));
        let args: serde_json::Value =
            serde_json::from_str(calls[1].args_json.as_deref().unwrap()).unwrap();
        assert_eq!(args["command"], "cargo test");
        assert_eq!(session_tool_calls(&conn, "s1", None).unwrap().len(), 3);

        let edits = session_file_edits(&conn, "s1", Some("claude")).unwrap();
        assert_eq!(
            edits
                .iter()
                .map(|e| e.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["/tmp/p/a.rs", "/tmp/p/b.rs"]
        );
        assert_eq!(edits[0].tool_name.as_deref(), Some("Write"));
        assert_eq!(edits[0].lines_added, Some(10));
        assert_eq!(edits[0].lines_removed, Some(0));
        assert_eq!(edits[0].user_modified, Some(1));
        assert_eq!(session_file_edits(&conn, "s1", None).unwrap().len(), 3);
        assert!(session_file_edits(&conn, "nope", None).unwrap().is_empty());
    }

    /// Two providers whose native session ids collide, one shared timestamp
    /// per table, and an undated tail: the shapes every page assertion below
    /// depends on.
    fn seed_evidence(conn: &Connection) {
        conn.execute_batch(
            r#"
            INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name, target, args_json, is_error, ts_ms)
            VALUES ('claude', 's1', 'm1', 'tu-a', 'Bash', 'cargo test', '{"command":"cargo test"}', 1, 200),
                   ('claude', 's1', 'm1', 'tu-b', 'Read', '/tmp/p/a.rs', '{"file_path":"/tmp/p/a.rs"}', 0, 100),
                   ('claude', 's1', 'm2', 'tu-c', 'Grep', 'needle', '{"pattern":"needle"}', NULL, 200),
                   ('claude', 's1', 'm2', 'tu-d', 'Glob', '*.rs', '{"pattern":"*.rs"}', 0, NULL),
                   ('claude', 's1', 'm3', 'tu-e', 'Write', '/tmp/p/b.rs', 'not json', 0, 100),
                   ('claude', 's1', 'm3', 'tu-f', 'Edit', '/tmp/p/c.rs', NULL, NULL, NULL),
                   ('codex', 's1', 'm9', 'tu-x', 'Shell', 'ls', '{}', 0, 150),
                   ('claude', 's2', 'm8', 'tu-y', 'Read', '/tmp/q/a.rs', '{}', 0, 150);
            INSERT INTO file_edits (source, session_id, message_id, tool_use_id, file_path, tool_name, lines_added, lines_removed, structured_patch_json, user_modified, ts_ms, git_branch, cwd)
            VALUES ('claude', 's1', 'm1', 'fe-a', '/tmp/p/a.rs', 'Write', 10, 0, '[{"lines":["+a"]}]', 1, 200, 'main', '/tmp/p'),
                   ('claude', 's1', 'm1', 'fe-b', '/tmp/p/b.rs', 'Edit', 2, 1, 'not json', 0, 100, 'main', '/tmp/p'),
                   ('claude', 's1', 'm2', 'fe-c', '/tmp/p/c.rs', 'Edit', 1, 1, NULL, NULL, 200, NULL, NULL),
                   ('claude', 's1', 'm2', 'fe-d', '/tmp/p/d.rs', 'Edit', 0, 0, NULL, NULL, NULL, NULL, NULL),
                   ('claude', 's1', 'm3', 'fe-e', '/tmp/p/e.rs', 'Edit', 3, 3, NULL, 1, 100, 'main', '/tmp/p'),
                   ('claude', 's1', 'm3', 'fe-f', '/tmp/p/f.rs', 'Edit', 0, 1, NULL, NULL, NULL, NULL, NULL),
                   ('codex', 's1', 'm9', 'fe-x', '/tmp/p/z.rs', 'apply_patch', 1, 1, NULL, NULL, 150, NULL, NULL),
                   ('claude', 's2', 'm8', 'fe-y', '/tmp/q/a.rs', 'Edit', 1, 0, NULL, NULL, 150, NULL, NULL);
            "#,
        )
        .unwrap();
    }

    /// Every page of a full walk, plus the ids it yielded in order. Pages are
    /// bounded, so a cursor that failed to advance would hang rather than
    /// fail; the iteration guard turns that into an assertion.
    fn walk_tool_calls(conn: &Connection, source: &str, session_id: &str, limit: i64) -> Vec<i64> {
        let mut ids = Vec::new();
        let mut cursor = None;
        for _ in 0..100 {
            let page =
                session_tool_calls_page(conn, source, session_id, limit, cursor.as_ref()).unwrap();
            assert!(page.tool_calls.len() as i64 <= limit.clamp(1, 1_000));
            ids.extend(page.tool_calls.iter().map(|call| call.id));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => return ids,
            }
        }
        panic!("tool call pagination did not terminate");
    }

    fn walk_file_edits(conn: &Connection, source: &str, session_id: &str, limit: i64) -> Vec<i64> {
        let mut ids = Vec::new();
        let mut cursor = None;
        for _ in 0..100 {
            let page =
                session_file_edits_page(conn, source, session_id, limit, cursor.as_ref()).unwrap();
            assert!(page.file_edits.len() as i64 <= limit.clamp(1, 1_000));
            ids.extend(page.file_edits.iter().map(|edit| edit.id));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => return ids,
            }
        }
        panic!("file edit pagination did not terminate");
    }

    #[test]
    fn evidence_pages_scope_to_one_source_and_session() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        seed_evidence(&conn);

        let calls = session_tool_calls_page(&conn, "claude", "s1", 100, None).unwrap();
        assert!(calls
            .tool_calls
            .iter()
            .all(|call| call.source == "claude" && call.session_id == "s1"));
        assert_eq!(calls.tool_calls.len(), 6);
        assert_eq!(
            session_tool_calls_page(&conn, "codex", "s1", 100, None)
                .unwrap()
                .tool_calls
                .iter()
                .map(|call| call.tool_use_id.as_str())
                .collect::<Vec<_>>(),
            vec!["tu-x"]
        );

        let edits = session_file_edits_page(&conn, "claude", "s1", 100, None).unwrap();
        assert!(edits
            .file_edits
            .iter()
            .all(|edit| edit.source == "claude" && edit.session_id == "s1"));
        assert_eq!(edits.file_edits.len(), 6);
        let first = &edits.file_edits[0];
        assert_eq!(first.tool_use_id, "fe-b");
        assert_eq!(first.message_id.as_deref(), Some("m1"));
        assert_eq!(first.structured_patch_json.as_deref(), Some("not json"));
        assert_eq!(first.git_branch.as_deref(), Some("main"));
        assert_eq!(first.cwd.as_deref(), Some("/tmp/p"));
        assert_eq!(
            session_file_edits_page(&conn, "codex", "s1", 100, None)
                .unwrap()
                .file_edits
                .iter()
                .map(|edit| edit.tool_use_id.as_str())
                .collect::<Vec<_>>(),
            vec!["fe-x"]
        );
    }

    #[test]
    fn evidence_pages_walk_tied_timestamps_and_undated_tails_without_gaps() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        seed_evidence(&conn);

        let expected_calls = session_tool_calls(&conn, "s1", Some("claude"))
            .unwrap()
            .iter()
            .map(|call| call.id)
            .collect::<Vec<_>>();
        let expected_edits = session_file_edits(&conn, "s1", Some("claude"))
            .unwrap()
            .iter()
            .map(|edit| edit.id)
            .collect::<Vec<_>>();
        // Tied timestamps ahead of an undated tail: the order the unpaginated
        // readers already promise.
        assert_eq!(expected_calls.len(), 6);
        assert_eq!(expected_edits.len(), 6);

        for limit in [1, 2, 3, 5, 6, 7, 1_000] {
            assert_eq!(
                walk_tool_calls(&conn, "claude", "s1", limit),
                expected_calls,
                "tool calls at limit {limit}"
            );
            assert_eq!(
                walk_file_edits(&conn, "claude", "s1", limit),
                expected_edits,
                "file edits at limit {limit}"
            );
        }
    }

    #[test]
    fn evidence_page_cursors_are_exact_at_page_boundaries() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        seed_evidence(&conn);

        // A page that exactly consumes the rows reports no continuation.
        let exact = session_tool_calls_page(&conn, "claude", "s1", 6, None).unwrap();
        assert_eq!(exact.tool_calls.len(), 6);
        assert_eq!(exact.next_cursor, None);

        // A cursor taken mid-tie resumes at the tie's next row, not after it.
        let first = session_tool_calls_page(&conn, "claude", "s1", 1, None).unwrap();
        let cursor = first.next_cursor.clone().unwrap();
        assert_eq!(cursor.ts_ms, Some(100));
        let second = session_tool_calls_page(&conn, "claude", "s1", 1, Some(&cursor)).unwrap();
        assert_eq!(second.tool_calls[0].ts_ms, Some(100));
        assert!(second.tool_calls[0].id > first.tool_calls[0].id);

        // A cursor on the last dated row admits the whole undated tail.
        let dated = session_tool_calls_page(&conn, "claude", "s1", 4, None).unwrap();
        let tail = session_tool_calls_page(&conn, "claude", "s1", 100, dated.next_cursor.as_ref())
            .unwrap();
        assert!(tail.tool_calls.iter().all(|call| call.ts_ms.is_none()));
        assert_eq!(tail.tool_calls.len(), 2);
        assert_eq!(tail.next_cursor, None);

        // A cursor already inside the undated tail never walks back into the
        // dated head.
        let undated = SessionEvidenceCursor {
            ts_ms: None,
            id: tail.tool_calls[0].id,
        };
        let rest = session_file_edits_page(&conn, "claude", "s1", 100, None).unwrap();
        let last_undated = rest.file_edits.last().unwrap();
        let after_all = session_file_edits_page(
            &conn,
            "claude",
            "s1",
            100,
            Some(&SessionEvidenceCursor {
                ts_ms: last_undated.ts_ms,
                id: last_undated.id,
            }),
        )
        .unwrap();
        assert!(after_all.file_edits.is_empty());
        assert_eq!(after_all.next_cursor, None);
        let continued =
            session_tool_calls_page(&conn, "claude", "s1", 100, Some(&undated)).unwrap();
        assert_eq!(continued.tool_calls.len(), 1);
        assert!(continued.tool_calls[0].ts_ms.is_none());
    }

    #[test]
    fn evidence_pages_clamp_limits_and_read_unknown_sessions_empty() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        seed_evidence(&conn);

        // Out-of-range limits clamp instead of erroring or returning nothing.
        assert_eq!(
            session_tool_calls_page(&conn, "claude", "s1", 0, None)
                .unwrap()
                .tool_calls
                .len(),
            1
        );
        assert_eq!(
            session_file_edits_page(&conn, "claude", "s1", -5, None)
                .unwrap()
                .file_edits
                .len(),
            1
        );
        assert_eq!(
            session_tool_calls_page(&conn, "claude", "s1", 10_000, None)
                .unwrap()
                .tool_calls
                .len(),
            6
        );

        for (source, session_id) in [("claude", "nope"), ("nope", "s1")] {
            let calls = session_tool_calls_page(&conn, source, session_id, 10, None).unwrap();
            assert!(calls.tool_calls.is_empty());
            assert_eq!(calls.next_cursor, None);
            let edits = session_file_edits_page(&conn, source, session_id, 10, None).unwrap();
            assert!(edits.file_edits.is_empty());
            assert_eq!(edits.next_cursor, None);
        }
    }

    /// A database predating the evidence page indexes has outstanding
    /// migration work: the read guard must refuse it so the caller is routed
    /// through the writable open that creates them, rather than serving the
    /// page from a full table scan.
    #[test]
    fn evidence_page_indexes_migrate_onto_an_existing_database() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        assert!(schema_is_evidence_read_current(&conn).unwrap());

        conn.execute_batch("DROP INDEX idx_tool_calls_page_v2; DROP INDEX idx_file_edits_page_v2;")
            .unwrap();
        assert!(!schema_is_evidence_read_current(&conn).unwrap());
        assert!(!schema_is_current(&conn).unwrap());
        // Every unrelated read path keeps its read-only handle while that
        // migration is pending: each is gated on its own scoped guard, so
        // only the evidence pages and the next writable open see the gap.
        assert!(schema_is_event_read_current(&conn).unwrap());
        assert!(schema_is_catalog_read_current(&conn).unwrap());
        assert!(schema_is_read_current(&conn).unwrap());

        // One writable open migrates it, exactly as the session-events page
        // indexes did when they joined the required list.
        init_db(&conn).unwrap();
        assert!(schema_is_evidence_read_current(&conn).unwrap());
        assert!(schema_is_current(&conn).unwrap());
    }

    /// A database carrying the first shape of the page indexes -- the same
    /// names over the bare `ts_ms` column -- must be upgraded, not mistaken
    /// for current. The guard compares names, so the corrected shape took new
    /// names and the originals joined the retirement list.
    #[test]
    fn the_superseded_evidence_indexes_are_dropped_and_replaced() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute_batch(
            "DROP INDEX idx_tool_calls_page_v2;
             DROP INDEX idx_file_edits_page_v2;
             CREATE INDEX idx_tool_calls_page ON tool_calls(source, session_id, ts_ms, id);
             CREATE INDEX idx_file_edits_page ON file_edits(source, session_id, ts_ms, id);
             CREATE INDEX idx_tool_calls_session ON tool_calls(source, session_id);
             CREATE INDEX idx_file_edits_session ON file_edits(source, session_id);",
        )
        .unwrap();
        assert!(!schema_is_evidence_read_current(&conn).unwrap());
        assert!(!schema_is_current(&conn).unwrap());

        init_db(&conn).unwrap();
        assert!(schema_is_evidence_read_current(&conn).unwrap());
        assert!(schema_is_current(&conn).unwrap());
        for retired in [
            "idx_tool_calls_page",
            "idx_file_edits_page",
            "idx_tool_calls_session",
            "idx_file_edits_session",
        ] {
            assert!(
                RETIRED_INDEXES.contains(&retired),
                "{retired} must be dropped from existing databases"
            );
            let present: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?)",
                    [retired],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(!present, "{retired} is still present after migration");
        }
    }

    /// The evidence page promises an indexed, sort-free read on all three
    /// query shapes: the first page, a dated continuation, and a continuation
    /// already inside the undated tail. The last one is the shape that walks
    /// the longest, so a temp b-tree there would re-sort the remaining tail on
    /// every page. The plan is taken from the same builder the readers run, so
    /// it cannot pass against a restated copy of the query.
    ///
    /// Each shape is checked with and without `sqlite_stat1`: the repository
    /// never runs `ANALYZE`, but a user's database may carry stats, and the
    /// planner picks differently once it has them.
    #[test]
    fn evidence_pages_are_served_by_an_index_without_sorting() {
        for analyze in [false, true] {
            let conn = Connection::open_in_memory().unwrap();
            init_db(&conn).unwrap();
            seed_evidence(&conn);
            if analyze {
                conn.execute_batch("ANALYZE").unwrap();
            }

            let plan = |table: &str, columns: &str, after: Option<&SessionEvidenceCursor>| {
                let (sql, params) = evidence_page_query(columns, table, "claude", "s1", 10, after);
                let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
                stmt.query_map(rusqlite::params_from_iter(params), |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
                .join(" | ")
            };

            for (table, columns, index) in [
                ("tool_calls", TOOL_CALL_COLUMNS, "idx_tool_calls_page_v2"),
                ("file_edits", FILE_EDIT_COLUMNS, "idx_file_edits_page_v2"),
            ] {
                for (shape, after) in [
                    ("the first page", None),
                    (
                        "a dated continuation",
                        Some(SessionEvidenceCursor {
                            ts_ms: Some(100),
                            id: 1,
                        }),
                    ),
                    (
                        "an undated continuation",
                        Some(SessionEvidenceCursor { ts_ms: None, id: 1 }),
                    ),
                ] {
                    let plan = plan(table, columns, after.as_ref());
                    assert!(
                        plan.contains(index)
                            && !plan.contains("TEMP B-TREE")
                            && !plan.contains("SCAN"),
                        "{shape} of {table} must be an index-ordered search \
                         (analyze={analyze}): {plan}"
                    );
                }
            }
        }
    }

    #[test]
    fn evidence_pages_return_stored_json_verbatim() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        seed_evidence(&conn);

        // The core never parses stored provider JSON: unparseable and absent
        // values are distinct, and both reach the caller intact.
        let calls = session_tool_calls_page(&conn, "claude", "s1", 100, None).unwrap();
        let by_id = |tool_use_id: &str| {
            calls
                .tool_calls
                .iter()
                .find(|call| call.tool_use_id == tool_use_id)
                .unwrap()
        };
        assert_eq!(by_id("tu-e").args_json.as_deref(), Some("not json"));
        assert_eq!(by_id("tu-f").args_json, None);
        assert_eq!(by_id("tu-a").is_error, Some(1));
        assert_eq!(by_id("tu-b").is_error, Some(0));
        assert_eq!(by_id("tu-c").is_error, None);

        let edits = session_file_edits_page(&conn, "claude", "s1", 100, None).unwrap();
        let patched = edits
            .file_edits
            .iter()
            .find(|edit| edit.tool_use_id == "fe-a")
            .unwrap();
        assert_eq!(
            patched.structured_patch_json.as_deref(),
            Some(r#"[{"lines":["+a"]}]"#)
        );
        assert_eq!(patched.user_modified, Some(1));
    }

    #[test]
    fn init_db_uses_wal_and_legacy_session_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fresh.db");
        let conn = open_db(&db_path).unwrap();
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode, "wal");

        let history_cols = conn
            .prepare("PRAGMA table_info(history)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(history_cols.contains(&"git_branch".to_string()));

        let sessions_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sessions'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sessions_exists, 1);
    }

    /// A database that is schema-current but still carries a retired index
    /// has outstanding migration work: the drop must run (inside the
    /// transactional pass, not the lock-free fast path) so existing databases
    /// shed the write amplification on their next open.
    #[test]
    fn retired_sessions_indexes_are_dropped_from_a_current_database() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute("CREATE INDEX idx_sessions_cwd ON sessions(cwd)", [])
            .unwrap();
        assert!(schema_is_current(&conn).unwrap());
        assert!(retired_indexes_present(&conn).unwrap());

        init_db(&conn).unwrap();
        assert!(!retired_indexes_present(&conn).unwrap());
    }

    /// `session_markers` has to reach a database written before it existed,
    /// and a read-only handle must refuse to serve that database until it
    /// does — otherwise the first read of a Grok compaction boundary fails
    /// with `no such table` instead of migrating. The migration is additive:
    /// nothing already in the database is touched.
    #[test]
    fn session_markers_migrate_onto_a_database_that_predates_them() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("pre-markers.db");
        {
            let conn = open_db(&db_path).unwrap();
            conn.execute(
                "INSERT INTO sessions (session_id, source, last_activity_ms)                  VALUES ('kept-1', 'grok', 7)",
                [],
            )
            .unwrap();
            // Rewind the database to before this change shipped.
            conn.execute_batch(
                "DROP TRIGGER delete_session_markers;
                 DROP TABLE session_markers;
                 DELETE FROM schema_migrations WHERE name = 'session_markers_v1';",
            )
            .unwrap();
            assert!(
                !schema_is_current(&conn).unwrap(),
                "a database without session_markers must not be served read-only"
            );
        }

        let conn = open_db(&db_path).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        insert_session_marker(
            &conn,
            &SessionMarker {
                source: "grok".into(),
                session_id: "kept-1".into(),
                marker_uid: "compaction:1.json".into(),
                kind: "compaction_boundary".into(),
                message_id: None,
                ts_ms: Some(11),
                text: None,
                detail_json: None,
            },
        )
        .unwrap();
        // Re-reading the same evidence updates the row rather than adding one.
        insert_session_marker(
            &conn,
            &SessionMarker {
                source: "grok".into(),
                session_id: "kept-1".into(),
                marker_uid: "compaction:1.json".into(),
                kind: "compaction_boundary".into(),
                message_id: None,
                ts_ms: Some(12),
                text: Some("second read".into()),
                detail_json: None,
            },
        )
        .unwrap();
        let markers = session_markers(&conn, "grok", "kept-1").unwrap();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].ts_ms, Some(12));
        assert_eq!(markers[0].text.as_deref(), Some("second read"));
        // The row that was already there survived the migration untouched.
        let kept: i64 = conn
            .query_row(
                "SELECT last_activity_ms FROM sessions WHERE session_id = 'kept-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(kept, 7);

        // Deleting the session takes its markers with it.
        conn.execute("DELETE FROM sessions WHERE session_id = 'kept-1'", [])
            .unwrap();
        assert!(session_markers(&conn, "grok", "kept-1").unwrap().is_empty());
    }

    /// The project-identity columns, marker and index reach a database that
    /// predates them — and the migration deliberately invents no keys.
    ///
    /// A backfill that stamped every existing `cwd` as a path key would be the
    /// worst outcome: the row would read as "resolved, and this repository has
    /// no remote" forever, every consumer grouping by the key would split that
    /// repository across its checkouts, and nothing downstream could tell that
    /// answer from a real one. NULL says "not resolved yet" and the next sync
    /// or hydration resolves it for real.
    #[test]
    fn project_identity_migrates_onto_an_older_database_without_inventing_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("pre-project-key.db");
        {
            let legacy = Connection::open(&db_path).unwrap();
            legacy.execute_batch(SCHEMA).unwrap();
            // The `sessions` shape as it stood before this change, plus a row
            // whose working directory is a real repository.
            legacy
                .execute_batch(
                    "DROP TABLE IF EXISTS sessions;
                     CREATE TABLE sessions (
                         session_id TEXT NOT NULL,
                         source TEXT NOT NULL,
                         cwd TEXT,
                         git_branch TEXT,
                         first_activity_ms INTEGER,
                         last_activity_ms INTEGER,
                         last_assistant_text TEXT,
                         raw_path TEXT,
                         parser_version INTEGER NOT NULL DEFAULT 1,
                         PRIMARY KEY (session_id, source)
                     );
                     INSERT INTO sessions (session_id, source, cwd, last_activity_ms)
                     VALUES ('legacy-1', 'claude', '/work/app', 42);",
                )
                .unwrap();
            // `session_events` predates the column too.
            legacy
                .execute_batch(
                    "INSERT INTO session_events \
                     (source, session_id, ts_ms, role, kind, text, event_uid) \
                     VALUES ('claude', 'legacy-1', 42, 'user', 'text', 'hi', 'e1');",
                )
                .unwrap();
            legacy
                .execute("ALTER TABLE session_events DROP COLUMN project_key", [])
                .unwrap();
            assert!(
                !schema_is_current(&legacy).unwrap(),
                "a database without the project-identity columns must not be served read-only"
            );
        }

        let conn = open_db(&db_path).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        let has_column = |table: &str, column: &str| -> bool {
            conn.prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
                .unwrap()
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<HashSet<String>>>()
                .unwrap()
                .contains(column)
        };
        assert!(has_column("sessions", "project_key"));
        assert!(has_column("sessions", "project_key_method"));
        assert!(has_column("session_events", "project_key"));
        assert!(conn
            .prepare("SELECT 1 FROM schema_migrations WHERE name = 'session_project_key_v1'")
            .unwrap()
            .exists([])
            .unwrap());
        assert!(conn
            .prepare(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' \
                 AND name = 'idx_sessions_project_key'"
            )
            .unwrap()
            .exists([])
            .unwrap());

        // The migration itself resolved nothing: the existing row keeps a
        // null key rather than being told its project is a directory.
        let stored: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT project_key, project_key_method FROM sessions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, (None, None));
        let event_key: Option<String> = conn
            .query_row("SELECT project_key FROM session_events", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(event_key, None);

        // `/work/app` does not exist, so the refresh resolves it to itself and
        // says `path` — a fallback that announces itself, not a remote.
        assert!(refresh_project_identity(&conn).unwrap() > 0);
        let resolved: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT project_key, project_key_method FROM sessions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            resolved,
            (
                Some("/work/app".to_string()),
                Some(
                    crate::project_identity::ProjectKeyMethod::PathFallback
                        .as_str()
                        .to_string()
                )
            )
        );
        // And the denormalized copy followed.
        let event_key: Option<String> = conn
            .query_row("SELECT project_key FROM session_events", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(event_key.as_deref(), Some("/work/app"));
    }

    /// The catalog columns must reach a database that predates them, and a
    /// read-only handle must refuse to serve that database until they do —
    /// otherwise `sessions list` fails with `no such column` instead of
    /// migrating.
    #[test]
    fn session_catalog_columns_migrate_onto_a_pre_catalog_database() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("legacy.db");
        {
            let legacy = Connection::open(&db_path).unwrap();
            legacy.execute_batch(SCHEMA).unwrap();
            legacy
                .execute_batch(
                    "ALTER TABLE history ADD COLUMN git_branch TEXT;
                     CREATE TABLE sessions (
                         session_id TEXT NOT NULL,
                         source TEXT NOT NULL,
                         cwd TEXT,
                         git_branch TEXT,
                         first_activity_ms INTEGER,
                         last_activity_ms INTEGER,
                         last_assistant_text TEXT,
                         raw_path TEXT,
                         parser_version INTEGER NOT NULL DEFAULT 1,
                         PRIMARY KEY (session_id, source)
                     );
                     INSERT INTO sessions (session_id, source, last_activity_ms)
                     VALUES ('legacy-1', 'claude', 42);
                     INSERT INTO history (source, session_id, prompt, timestamp_ms)
                     VALUES ('codex', 'history-only', 'legacy prompt', 41);",
                )
                .unwrap();
            assert!(
                !schema_is_current(&legacy).unwrap(),
                "a database without the catalog columns must not be served read-only"
            );
        }

        let conn = open_db(&db_path).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        let columns = conn
            .prepare("SELECT name FROM pragma_table_info('sessions')")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<HashSet<String>>>()
            .unwrap();
        for needed in REQUIRED_SESSIONS_COLUMNS {
            assert!(columns.contains(*needed), "missing column {needed}");
        }
        // Existing rows survive the migration with the new columns null.
        let (id, state): (String, Option<String>) = conn
            .query_row(
                "SELECT session_id, discovery_state FROM sessions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(id, "legacy-1");
        assert_eq!(state, None);
        let location: String = conn
            .query_row(
                "SELECT location FROM session_presences WHERE source = 'claude' AND session_id = 'legacy-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(location, "local");
        let history_location: String = conn
            .query_row(
                "SELECT location FROM session_presences WHERE source = 'codex' AND session_id = 'history-only'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(history_location, "local");

        // The legacy backfill is permanently guarded. A session discovered
        // remotely after migration remains remote-only on every later init.
        conn.execute(
            "INSERT INTO sessions (session_id, source) VALUES ('cloud-1', 'codex')",
            [],
        )
        .unwrap();
        mark_session_presence(&conn, "codex", "cloud-1", SessionLocation::Remote).unwrap();
        init_db(&conn).unwrap();
        let locations = conn
            .prepare(
                "SELECT location FROM session_presences WHERE source = 'codex' AND session_id = 'cloud-1' ORDER BY location",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(locations, ["remote"]);

        conn.execute(
            "DELETE FROM sessions WHERE source = 'codex' AND session_id = 'cloud-1'",
            [],
        )
        .unwrap();
        let stale: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_presences WHERE source = 'codex' AND session_id = 'cloud-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stale, 0, "canonical deletion cleans presence state");
    }

    #[test]
    fn concurrent_first_presence_migrations_are_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("concurrent-legacy.db");
        {
            let legacy = Connection::open(&db_path).unwrap();
            legacy.execute_batch(SCHEMA).unwrap();
            legacy
                .execute_batch(
                    "ALTER TABLE history ADD COLUMN git_branch TEXT;
                     CREATE TABLE sessions (
                         session_id TEXT NOT NULL,
                         source TEXT NOT NULL,
                         cwd TEXT,
                         git_branch TEXT,
                         first_activity_ms INTEGER,
                         last_activity_ms INTEGER,
                         last_assistant_text TEXT,
                         raw_path TEXT,
                         parser_version INTEGER NOT NULL DEFAULT 1,
                         PRIMARY KEY (session_id, source)
                     );
                     INSERT INTO sessions (session_id, source)
                     VALUES ('legacy-race', 'claude');",
                )
                .unwrap();
        }

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let handles = (0..4)
            .map(|_| {
                let barrier = barrier.clone();
                let path = db_path.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    open_db(&path).map(drop)
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        let conn = open_db(&db_path).unwrap();
        let marker_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM schema_migrations WHERE name = 'session_presences_local_backfill_v1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(marker_count, 1);
        let presence_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_presences WHERE source = 'claude' AND session_id = 'legacy-race' AND location = 'local'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(presence_count, 1);
    }

    /// The catalog listing's promise is an indexed, sort-free read. A database
    /// that has the columns but not the indexes would be served read-only with
    /// degraded plans, silently, so the guard covers them too.
    #[test]
    fn a_database_missing_a_catalog_index_is_not_served_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("indexless.db");
        let conn = open_db(&db_path).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        // Named explicitly so dropping one from the guard is a deliberate act:
        // the recency pair carries the listing's sort-free order, and
        // idx_sessions_raw_path carries discovery's per-candidate "has this
        // transcript changed?" lookup.
        for needed in [
            "idx_sessions_recency",
            "idx_sessions_source_recency",
            "idx_sessions_raw_path",
        ] {
            assert!(
                REQUIRED_INDEXES.contains(&needed),
                "{needed} is load-bearing and must stay in the guard"
            );
        }
        for index in REQUIRED_INDEXES {
            conn.execute_batch(&format!("DROP INDEX {index}")).unwrap();
            assert!(
                !schema_is_current(&conn).unwrap(),
                "{index} is load-bearing for the catalog listing"
            );
            // init_db is idempotent, so the writable path heals it.
            init_db(&conn).unwrap();
            assert!(schema_is_current(&conn).unwrap());
        }
    }

    /// Delivery is an optional feature, and that is what opens the hole. A
    /// database can carry delivery tables and capture triggers from a
    /// delivery-enabled build, then be opened by a `--no-default-features`
    /// build: that build adds the new `session_events` columns, because the
    /// migration is not delivery-gated, but it never rebuilds the triggers,
    /// because `init_delivery_schema` is compiled out. On the next
    /// delivery-enabled open the trigger names are all still present, so a
    /// read-only fast path that checks names alone is satisfied, `init_db`
    /// never reaches the rebuild, and delivery goes on reporting success while
    /// the new fields never leave the machine.
    ///
    /// So a name is not enough: the payload has to be checked too.
    ///
    #[cfg(feature = "delivery")]
    #[test]
    fn a_capture_trigger_missing_provider_is_not_current() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("stale-trigger.db");
        let conn = open_db(&db_path).unwrap();
        assert!(schema_is_current(&conn).unwrap());

        let capture_sql = |conn: &Connection| -> String {
            conn.query_row(
                "SELECT sql FROM sqlite_master \
                 WHERE type='trigger' AND name='delivery_session_events_insert'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };

        // Rewrite the trigger to the shape a build before these columns left
        // behind: same name, payload one column short.
        let omitted = "'provider',NEW.\"provider\"";
        let sql = capture_sql(&conn);
        assert!(
            sql.contains(omitted),
            "the payload must carry {omitted} before removing it, or this test proves nothing"
        );
        let stale_sql = sql.replace(&format!(",{omitted}"), "");
        assert_ne!(stale_sql, sql);
        conn.execute_batch("DROP TRIGGER delivery_session_events_insert;")
            .unwrap();
        conn.execute_batch(&stale_sql).unwrap();

        assert!(
            !schema_is_current(&conn).unwrap(),
            "a trigger whose payload predates a column its table has is not current"
        );

        // And the writable open repairs it, which is the point of saying so.
        drop(conn);
        let conn = open_db(&db_path).unwrap();
        assert!(capture_sql(&conn).contains(omitted));
        assert!(schema_is_current(&conn).unwrap());
    }

    #[test]
    fn read_schema_does_not_require_the_discovery_only_raw_path_index() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute_batch("DROP INDEX idx_sessions_raw_path")
            .unwrap();

        assert!(schema_is_read_current(&conn).unwrap());
        assert!(schema_is_catalog_read_current(&conn).unwrap());
        assert!(schema_is_event_read_current(&conn).unwrap());
        assert!(!schema_is_current(&conn).unwrap());

        conn.execute_batch("DROP INDEX idx_session_events_page")
            .unwrap();
        assert!(schema_is_read_current(&conn).unwrap());
        assert!(!schema_is_event_read_current(&conn).unwrap());

        init_db(&conn).unwrap();
        conn.execute_batch("DROP INDEX idx_session_presences_location")
            .unwrap();
        assert!(!schema_is_current(&conn).unwrap());
        assert!(!schema_is_read_current(&conn).unwrap());
        assert!(!schema_is_catalog_read_current(&conn).unwrap());
        // Direct event pagination does not use session location.
        assert!(schema_is_event_read_current(&conn).unwrap());
    }

    /// A fresh database and a migrated one must end up with the same `sessions`
    /// shape, or the CREATE TABLE and the ALTER TABLE list have drifted apart.
    #[test]
    fn fresh_and_migrated_session_tables_converge() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = open_db(&dir.path().join("fresh.db")).unwrap();
        let legacy_path = dir.path().join("legacy.db");
        {
            let legacy = Connection::open(&legacy_path).unwrap();
            legacy
                .execute_batch(
                    "CREATE TABLE sessions (
                         session_id TEXT NOT NULL,
                         source TEXT NOT NULL,
                         cwd TEXT,
                         git_branch TEXT,
                         first_activity_ms INTEGER,
                         last_activity_ms INTEGER,
                         last_assistant_text TEXT,
                         raw_path TEXT,
                         parser_version INTEGER NOT NULL DEFAULT 1,
                         PRIMARY KEY (session_id, source)
                     );",
                )
                .unwrap();
        }
        let migrated = open_db(&legacy_path).unwrap();
        let columns = |conn: &Connection| {
            conn.prepare("SELECT name FROM pragma_table_info('sessions') ORDER BY name")
                .unwrap()
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<String>>>()
                .unwrap()
        };
        assert_eq!(columns(&fresh), columns(&migrated));
    }

    #[test]
    fn event_pages_are_bounded_and_do_not_skip_timestamp_ties() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&dir.path().join("events.db")).unwrap();
        for index in 0..5 {
            conn.execute(
                "INSERT INTO session_events
                 (source, session_id, ts_ms, role, kind, text, event_uid)
                 VALUES ('codex', 'tied', 42, 'assistant', 'text', ?, ?)",
                params![format!("event-{index}"), format!("uid-{index}")],
            )
            .unwrap();
        }

        let first = session_events_page(&conn, "tied", Some("codex"), 2, None).unwrap();
        assert_eq!(first.events.len(), 2);
        let second =
            session_events_page(&conn, "tied", Some("codex"), 2, first.next_cursor.as_ref())
                .unwrap();
        assert_eq!(second.events.len(), 2);
        let third =
            session_events_page(&conn, "tied", Some("codex"), 2, second.next_cursor.as_ref())
                .unwrap();
        assert_eq!(third.events.len(), 1);
        assert!(third.next_cursor.is_none());

        let ids = first
            .events
            .into_iter()
            .chain(second.events)
            .chain(third.events)
            .map(|event| event.id)
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 5);
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn a_pre_continuity_database_gains_the_column_and_the_table_without_losing_rows() {
        // The v2 relationship shape as it stood before continuity: no
        // `origin_session_id`, no evidence table. Both additions have to land
        // on reopen, and the delegation row already there has to survive.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pre-continuity.db");
        let old = Connection::open(&path).unwrap();
        old.execute_batch(
            "CREATE TABLE schema_migrations (name TEXT PRIMARY KEY);
             INSERT INTO schema_migrations (name) VALUES ('session_relationships_v2');
             CREATE TABLE session_relationships (
                 source TEXT NOT NULL,
                 parent_session_id TEXT NOT NULL,
                 relationship_uid TEXT NOT NULL,
                 child_session_id TEXT,
                 relationship TEXT NOT NULL,
                 identity_status TEXT NOT NULL CHECK(identity_status IN ('observed','unlinked')),
                 child_agent_type TEXT,
                 child_agent_name TEXT,
                 child_model TEXT,
                 spawn_depth INTEGER,
                 evidence_kind TEXT NOT NULL,
                 evidence_locator TEXT,
                 evidence_ref TEXT,
                 child_has_events INTEGER NOT NULL DEFAULT 0,
                 spawned_at_ms INTEGER,
                 created_ms INTEGER NOT NULL,
                 updated_ms INTEGER NOT NULL,
                 PRIMARY KEY (source, parent_session_id, relationship_uid)
             );
             INSERT INTO session_relationships
               (source, parent_session_id, relationship_uid, child_session_id, relationship,
                identity_status, evidence_kind, child_has_events, created_ms, updated_ms)
             VALUES ('codex', 'root', 'child:kid', 'kid', 'delegated', 'observed',
                     'codex_session_meta', 1, 5, 5);",
        )
        .unwrap();
        assert!(!schema_is_current(&old).unwrap());
        drop(old);

        let conn = open_db(&path).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        let (child, origin): (String, Option<String>) = conn
            .query_row(
                "SELECT child_session_id, origin_session_id FROM session_relationships",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(child, "kid");
        assert_eq!(origin, None);
        let evidence_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(evidence_rows, 0);
    }

    #[test]
    fn deleting_a_session_takes_its_continuity_evidence_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        conn.execute(
            "INSERT INTO sessions (source, session_id) VALUES ('claude', 'gone')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_continuity_evidence \
             (source, locator, session_id, in_log_session_ids_json, explicit_targets_json, updated_ms) \
             VALUES ('claude', '/tmp/gone.jsonl', 'gone', '[]', '{}', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM sessions WHERE source = 'claude' AND session_id = 'gone'",
            [],
        )
        .unwrap();
        let left: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(left, 0);
    }

    /// A database whose `session_events` predates the per-message raw facts
    /// must not be served read-only against columns it does not have, and the
    /// writable open must add them without disturbing the rows already there.
    ///
    /// The database is dropped back to the pre-facts shape from a current one,
    /// so the only thing missing is these columns: a database that failed the
    /// guard for some older reason would prove nothing about this one.
    #[test]
    fn session_event_raw_fact_columns_migrate_onto_an_existing_database() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("legacy-events.db");
        {
            let legacy = open_db(&db_path).unwrap();
            assert!(schema_is_current(&legacy).unwrap());
            legacy
                .execute(
                    "INSERT INTO session_events \
                     (source, session_id, ts_ms, role, kind, text, event_uid) \
                     VALUES ('claude', 'legacy-session', 7, 'assistant', 'text', 'kept', 'uid-1')",
                    [],
                )
                .unwrap();
            drop_session_event_capture_triggers(&legacy);
            for (column, _) in REQUIRED_SESSION_EVENT_COLUMNS {
                legacy
                    .execute_batch(&format!("ALTER TABLE session_events DROP COLUMN {column};"))
                    .unwrap();
            }
            assert!(
                !schema_is_current(&legacy).unwrap(),
                "a database without the raw fact columns must not be served read-only"
            );
        }

        let conn = open_db(&db_path).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        let columns = conn
            .prepare("SELECT name FROM pragma_table_info('session_events')")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<HashSet<String>>>()
            .unwrap();
        for (needed, _) in REQUIRED_SESSION_EVENT_COLUMNS {
            assert!(columns.contains(*needed), "missing column {needed}");
        }
        assert!(conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = 'session_events_raw_facts_v1')",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap());
        // The pre-existing row survives, with the new facts null rather than
        // invented: nothing knows them until the transcript is re-parsed.
        let row: (String, Option<String>, Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT text, request_id, stop_reason, is_sidechain FROM session_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(row, ("kept".to_string(), None, None, None));
        // A capture trigger created before the migration would go on emitting
        // the old column list, so delivery would keep succeeding while these
        // facts never left the machine. Rebuilding it is part of the upgrade.
        let capture_sql = conn
            .query_row(
                "SELECT sql FROM sqlite_master \
                 WHERE type='trigger' AND name='delivery_session_events_insert'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok();
        if let Some(sql) = capture_sql {
            for (column, _) in REQUIRED_SESSION_EVENT_COLUMNS {
                assert!(
                    sql.contains(*column),
                    "the rebuilt capture trigger still omits {column}"
                );
            }
        }
    }

    /// Delivery is an optional feature, and that is what opens the hole. A
    /// database can carry delivery tables and capture triggers from a
    /// delivery-enabled build, then be opened by a `--no-default-features`
    /// build: that build adds the new `session_events` columns, because the
    /// migration is not delivery-gated, but it never rebuilds the triggers,
    /// because `init_delivery_schema` is compiled out. On the next
    /// delivery-enabled open the trigger names are all still present, so a
    /// read-only fast path that checks names alone is satisfied, `init_db`
    /// never reaches the rebuild, and delivery goes on reporting success while
    /// the new fields never leave the machine.
    ///
    /// So a name is not enough: the payload has to be checked too.
    #[cfg(feature = "delivery")]
    #[test]
    fn a_capture_trigger_with_an_outdated_payload_is_not_current() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("stale-trigger.db");
        let conn = open_db(&db_path).unwrap();
        assert!(schema_is_current(&conn).unwrap());

        let capture_sql = |conn: &Connection| -> String {
            conn.query_row(
                "SELECT sql FROM sqlite_master \
                 WHERE type='trigger' AND name='delivery_session_events_insert'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };

        // Rewrite the trigger to the shape a build before the raw facts left
        // behind: same name, payload one column short.
        let omitted = "'turn_id',NEW.\"turn_id\"";
        let sql = capture_sql(&conn);
        assert!(
            sql.contains(omitted),
            "the payload must carry {omitted} before removing it, or this test proves nothing"
        );
        let stale_sql = sql.replace(&format!(",{omitted}"), "");
        assert_ne!(stale_sql, sql);
        conn.execute_batch("DROP TRIGGER delivery_session_events_insert;")
            .unwrap();
        conn.execute_batch(&stale_sql).unwrap();

        assert!(
            !schema_is_current(&conn).unwrap(),
            "a trigger whose payload predates a column its table has is not current"
        );

        // And the writable open repairs it, which is the point of saying so.
        drop(conn);
        let conn = open_db(&db_path).unwrap();
        assert!(capture_sql(&conn).contains(omitted));
        assert!(schema_is_current(&conn).unwrap());
    }

    /// A legacy database predates the raw-fact columns, so its capture triggers
    /// never referenced them either; SQLite refuses to drop a column a trigger
    /// still names.
    fn drop_session_event_capture_triggers(conn: &Connection) {
        let names = conn
            .prepare(
                "SELECT name FROM sqlite_master \
                 WHERE type='trigger' AND tbl_name='session_events' AND name LIKE 'delivery_%'",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap();
        for name in names {
            conn.execute_batch(&format!("DROP TRIGGER {name};"))
                .unwrap();
        }
    }

    /// A fresh database and a migrated one must end up with the same
    /// `session_events` shape, or the CREATE TABLE and the ALTER TABLE list
    /// have drifted apart.
    #[test]
    fn fresh_and_migrated_session_event_tables_converge() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = open_db(&dir.path().join("fresh-events.db")).unwrap();
        let legacy_path = dir.path().join("legacy-events.db");
        {
            let legacy = open_db(&legacy_path).unwrap();
            drop_session_event_capture_triggers(&legacy);
            for (column, _) in REQUIRED_SESSION_EVENT_COLUMNS {
                legacy
                    .execute_batch(&format!("ALTER TABLE session_events DROP COLUMN {column};"))
                    .unwrap();
            }
        }
        let migrated = open_db(&legacy_path).unwrap();
        let columns = |conn: &Connection| {
            conn.prepare("SELECT name, type FROM pragma_table_info('session_events') ORDER BY name")
                .unwrap()
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<(String, String)>>>()
                .unwrap()
        };
        assert_eq!(columns(&fresh), columns(&migrated));
    }

    /// Both event reads select the same column list, so a page and a whole
    /// session read cannot disagree about what an event carries.
    #[test]
    fn event_reads_return_the_raw_message_facts() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&dir.path().join("facts.db")).unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid, \
              request_id, stop_reason, agent_version, is_sidechain, is_meta, turn_id) \
             VALUES ('claude', 's', 1, 'assistant', 'text', 'hi', 'uid-1', \
                     'req_1', 'end_turn', '2.1.96', 0, 1, 'turn_1')",
            [],
        )
        .unwrap();

        let whole = session_events(&conn, "s", None).unwrap();
        let page = session_events_page(&conn, "s", None, 10, None).unwrap();
        assert_eq!(whole, page.events);
        let event = &whole[0];
        assert_eq!(event.request_id.as_deref(), Some("req_1"));
        assert_eq!(event.stop_reason.as_deref(), Some("end_turn"));
        assert_eq!(event.agent_version.as_deref(), Some("2.1.96"));
        assert_eq!(event.is_sidechain, Some(0));
        assert_eq!(event.is_meta, Some(1));
        assert_eq!(event.turn_id.as_deref(), Some("turn_1"));
    }
}
