use anyhow::{Context, Result};
use rusqlite::{params, Connection, DatabaseName, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// WS-9 cloud-sync: local recall store → WS-1 convergence envelope (Agent Relay Loop).
pub mod convergence;
/// WS-9 cloud-sync increment 2a: outbox builder (local rows → batch, sync logic only).
pub mod outbox;

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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Stats {
    pub total: i64,
    pub by_source: Vec<(String, i64)>,
    pub by_project: Vec<(String, i64)>,
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
    event_uid TEXT NOT NULL,
    UNIQUE(source, session_id, event_uid)
);
CREATE VIRTUAL TABLE IF NOT EXISTS session_events_fts USING fts5(
    text, role, project, content='session_events', content_rowid='id'
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
    "tool_calls",
    "file_edits",
    "session_commit_links",
    "trajectories",
    "trajectory_fts",
    "tags",
    "session_tags",
    "sessions",
];
const REQUIRED_HISTORY_COLUMNS: &[&str] = &["prompt_hash", "git_branch"];

/// Whether this database already has everything [`init_db`] would add.
///
/// Read-only handles skip `init_db`, so an older database would otherwise be
/// queried against tables and columns that do not exist -- a user upgrading
/// from before `session_events` existed would get `no such table` on their
/// first search instead of a silent migration. Callers fall back to a writable
/// open (which migrates) when this returns false.
pub fn schema_is_current(conn: &Connection) -> Result<bool> {
    let mut table = conn.prepare("SELECT 1 FROM sqlite_master WHERE name = ? LIMIT 1")?;
    for name in REQUIRED_TABLES {
        if !table.exists([name])? {
            return Ok(false);
        }
    }
    let columns: HashSet<String> = conn
        .prepare("SELECT name FROM pragma_table_info('history')")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(REQUIRED_HISTORY_COLUMNS
        .iter()
        .all(|needed| columns.contains(*needed)))
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
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.execute_batch(SCHEMA)?;
    let _ = conn.execute("ALTER TABLE history ADD COLUMN prompt_hash TEXT", []);
    let _ = conn.execute("ALTER TABLE history ADD COLUMN git_branch TEXT", []);
    conn.execute_batch(
        r#"
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
    PRIMARY KEY (session_id, source)
);
"#,
    )?;
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
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_cwd ON sessions(cwd)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_branch ON sessions(git_branch)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_last ON sessions(last_activity_ms DESC)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_events_session ON session_events(source, session_id)",
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
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tool_calls_session ON tool_calls(source, session_id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_edits_session ON file_edits(source, session_id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_edits_path ON file_edits(file_path)",
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

pub fn insert_history(conn: &Connection, entry: &HistoryEntry) -> Result<usize> {
    Ok(conn.execute(
        "INSERT OR IGNORE INTO history (source, session_id, project, prompt, prompt_hash, timestamp_ms) VALUES (?, ?, ?, ?, ?, ?)",
        params![entry.source, entry.session_id, entry.project, entry.prompt, entry.prompt_hash, entry.timestamp_ms],
    )?)
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
    pub project: Option<String>,
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
    pub event_uid: String,
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
    pub tool_use_id: String,
    pub file_path: String,
    pub tool_name: Option<String>,
    pub lines_added: Option<i64>,
    pub lines_removed: Option<i64>,
    pub user_modified: Option<i64>,
    pub ts_ms: Option<i64>,
}

/// All normalized events for one session, oldest first. Rows sharing a
/// timestamp keep insertion order via the rowid tiebreaker.
pub fn session_events(
    conn: &Connection,
    session_id: &str,
    source: Option<&str>,
) -> Result<Vec<SessionEvent>> {
    let mut sql = "SELECT id, source, session_id, project, cwd, git_branch, message_id, parent_id,                    ts_ms, role, kind, text, model, token_json, event_uid                    FROM session_events WHERE session_id = ?"
        .to_string();
    let mut params_vec = vec![session_id.to_string()];
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    sql.push_str(" ORDER BY ts_ms IS NULL, ts_ms ASC, id ASC");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), |row| {
        Ok(SessionEvent {
            id: row.get(0)?,
            source: row.get(1)?,
            session_id: row.get(2)?,
            project: row.get(3)?,
            cwd: row.get(4)?,
            git_branch: row.get(5)?,
            message_id: row.get(6)?,
            parent_id: row.get(7)?,
            ts_ms: row.get(8)?,
            role: row.get(9)?,
            kind: row.get(10)?,
            text: row.get(11)?,
            model: row.get(12)?,
            token_json: row.get(13)?,
            event_uid: row.get(14)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn session_tool_calls(
    conn: &Connection,
    session_id: &str,
    source: Option<&str>,
) -> Result<Vec<SessionToolCall>> {
    let mut sql = "SELECT id, source, session_id, message_id, tool_use_id, name, target,                    args_json, is_error, ts_ms                    FROM tool_calls WHERE session_id = ?"
        .to_string();
    let mut params_vec = vec![session_id.to_string()];
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    sql.push_str(" ORDER BY ts_ms IS NULL, ts_ms ASC, id ASC");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), |row| {
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
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn session_file_edits(
    conn: &Connection,
    session_id: &str,
    source: Option<&str>,
) -> Result<Vec<SessionFileEdit>> {
    let mut sql = "SELECT id, source, session_id, tool_use_id, file_path, tool_name,                    lines_added, lines_removed, user_modified, ts_ms                    FROM file_edits WHERE session_id = ?"
        .to_string();
    let mut params_vec = vec![session_id.to_string()];
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    sql.push_str(" ORDER BY ts_ms IS NULL, ts_ms ASC, id ASC");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), |row| {
        Ok(SessionFileEdit {
            id: row.get(0)?,
            source: row.get(1)?,
            session_id: row.get(2)?,
            tool_use_id: row.get(3)?,
            file_path: row.get(4)?,
            tool_name: row.get(5)?,
            lines_added: row.get(6)?,
            lines_removed: row.get(7)?,
            user_modified: row.get(8)?,
            ts_ms: row.get(9)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn stats(conn: &Connection, tag: Option<&str>) -> Result<Stats> {
    let mut where_sql = String::new();
    let mut params_vec = Vec::new();
    if let Some(tag) = tag {
        where_sql = " WHERE EXISTS (SELECT 1 FROM session_tags st JOIN tags t ON t.id = st.tag_id WHERE st.source = h.source AND st.session_id = h.session_id AND t.name = ?)".into();
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
        let extra = if where_sql.is_empty() {
            "WHERE project IS NOT NULL".to_string()
        } else {
            format!("{where_sql} AND project IS NOT NULL")
        };
        let mut stmt = conn.prepare(&format!("SELECT project, COUNT(*) FROM history h {extra} GROUP BY project ORDER BY COUNT(*) DESC LIMIT 10"))?;
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
        first_timestamp_ms,
        last_timestamp_ms,
    })
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

pub fn sync_opencode_db(conn: &Connection, opencode_db: &Path) -> Result<usize> {
    if !opencode_db.exists() {
        return Ok(0);
    }
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
    let mut stmt = src.prepare(
        "SELECT s.id, s.directory, p.data, COALESCE(p.time_created, m.time_created, s.time_created) FROM part p JOIN message m ON m.id = p.message_id JOIN session s ON s.id = p.session_id WHERE json_extract(m.data, '$.role') = 'user' AND json_extract(p.data, '$.type') = 'text' ORDER BY p.time_created ASC",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut inserted = 0;
    for (session_id, project, data, timestamp_ms) in rows {
        let value: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();
        let prompt = value
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        if prompt.is_empty() {
            continue;
        }
        inserted += insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: "opencode".into(),
                session_id: Some(session_id),
                project,
                prompt: prompt.to_string(),
                prompt_hash: Some(prompt_hash(prompt)),
                timestamp_ms,
            },
        )?;
    }
    Ok(inserted)
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
    fn fts_query_matches_python_semantics() {
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
}
