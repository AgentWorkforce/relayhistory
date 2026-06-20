use anyhow::{Context, Result};
use rusqlite::{params, Connection, DatabaseName, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

pub const SOURCE_CHOICES: &[&str] = &[
    "claude",
    "codex",
    "cursor",
    "relay",
    "trajectory",
    "opencode",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryEntry {
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
"#;

pub fn default_db_path() -> PathBuf {
    std::env::var_os("AI_HIST_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));
            home.join(".local/share/ai-hist/ai-history.db")
        })
}

pub fn open_db(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    init_db(&conn)?;
    Ok(conn)
}

pub fn init_db(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA)?;
    let _ = conn.execute("ALTER TABLE history ADD COLUMN prompt_hash TEXT", []);
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

fn normalize_tag_name(name: &str) -> String {
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
    let query = build_fts_query(terms, raw_fts);
    let mut sql = "SELECT h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms FROM history_fts f JOIN history h ON f.rowid = h.id WHERE history_fts MATCH ?".to_string();
    let mut params_vec = vec![query];
    append_filters(&mut sql, &mut params_vec, filter, "h");
    sql.push_str(" ORDER BY h.timestamp_ms DESC LIMIT ?");
    params_vec.push(filter.limit.max(1).to_string());
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), row_to_entry)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
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
    src_live.backup(DatabaseName::Main, &tmp, None)?;
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
}
