//! Typed history and event search, independent of command-line formatting.
use crate::{normalize_tag_name, raw_fts_query_error, QueryFilter, SessionScope};
use anyhow::Result;
use rusqlite::Connection;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchRole {
    All,
    User,
    Assistant,
}

#[derive(Debug, Clone)]
pub struct SearchRow {
    pub id: i64,
    pub source: String,
    pub session_id: Option<String>,
    pub project: Option<String>,
    pub text: String,
    pub timestamp_ms: i64,
    pub role: String,
    pub kind: String,
    pub match_source: String,
}

pub fn search_all(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
) -> Result<Vec<SearchRow>> {
    let mut rows = Vec::new();
    if !matches!(role, SearchRole::Assistant) {
        rows.extend(search_history_rows(conn, terms, raw_fts, filter)?);
    }
    rows.extend(search_event_rows(conn, terms, raw_fts, filter, role)?);
    rows.sort_by(|a, b| {
        b.timestamp_ms
            .cmp(&a.timestamp_ms)
            .then_with(|| b.id.cmp(&a.id))
            .then_with(|| a.match_source.cmp(&b.match_source))
    });
    rows.truncate(filter.limit.max(1) as usize);
    Ok(rows)
}

fn search_history_rows(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
) -> Result<Vec<SearchRow>> {
    let mut params_vec = Vec::new();
    let mut sql = if terms.is_empty() {
        "SELECT h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms \
         FROM history h WHERE 1=1"
            .to_string()
    } else {
        params_vec.push(crate::build_fts_query(terms, raw_fts));
        "SELECT h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms \
         FROM history_fts f JOIN history h ON f.rowid = h.id WHERE history_fts MATCH ?"
            .to_string()
    };
    append_history_search_filters(&mut sql, &mut params_vec, filter, "h");
    sql.push_str(" ORDER BY h.timestamp_ms DESC LIMIT ?");
    params_vec.push(filter.limit.max(1).to_string());
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_vec), |row| {
            Ok(SearchRow {
                id: row.get(0)?,
                source: row.get(1)?,
                session_id: row.get(2)?,
                project: row.get(3)?,
                text: row.get(4)?,
                timestamp_ms: row.get(5)?,
                role: "user".to_string(),
                kind: "history".to_string(),
                match_source: "history".to_string(),
            })
        })
        .map_err(|error| raw_fts_query_error(raw_fts, error))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| raw_fts_query_error(raw_fts, error))?;
    Ok(rows)
}

fn search_event_rows(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
) -> Result<Vec<SearchRow>> {
    let mut params_vec = Vec::new();
    let mut sql = if terms.is_empty() {
        "SELECT e.id, e.source, e.session_id, e.project, COALESCE(e.text, ''), e.ts_ms, e.role, e.kind \
         FROM session_events e WHERE 1=1"
            .to_string()
    } else {
        params_vec.push(crate::build_fts_query(terms, raw_fts));
        "SELECT e.id, e.source, e.session_id, e.project, COALESCE(e.text, ''), e.ts_ms, e.role, e.kind \
         FROM session_events_fts f JOIN session_events e ON f.rowid = e.id WHERE session_events_fts MATCH ?"
            .to_string()
    };
    append_event_search_filters(&mut sql, &mut params_vec, filter, "e", role);
    sql.push_str(" ORDER BY e.ts_ms DESC LIMIT ?");
    params_vec.push(filter.limit.max(1).to_string());
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_vec), |row| {
            Ok(SearchRow {
                id: row.get(0)?,
                source: row.get(1)?,
                session_id: row.get(2)?,
                project: row.get(3)?,
                text: row.get(4)?,
                timestamp_ms: row.get(5)?,
                role: row.get(6)?,
                kind: row.get(7)?,
                match_source: "session_event".to_string(),
            })
        })
        .map_err(|error| raw_fts_query_error(raw_fts, error))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| raw_fts_query_error(raw_fts, error))?;
    Ok(rows)
}

fn append_history_search_filters(
    sql: &mut String,
    params: &mut Vec<String>,
    filter: &QueryFilter,
    alias: &str,
) {
    append_session_scope_filter(sql, filter.scope, alias);
    if let Some(source) = &filter.source {
        sql.push_str(&format!(" AND {alias}.source = ?"));
        params.push(source.clone());
    }
    if let Some(project) = &filter.project {
        sql.push_str(&format!(" AND {alias}.project LIKE ?"));
        params.push(format!("%{project}%"));
    }
    if let Some(tag) = &filter.tag {
        sql.push_str(&format!(" AND {}", tag_filter_clause(alias)));
        params.push(normalize_tag_name(tag));
    }
}

fn append_event_search_filters(
    sql: &mut String,
    params: &mut Vec<String>,
    filter: &QueryFilter,
    alias: &str,
    role: SearchRole,
) {
    append_session_scope_filter(sql, filter.scope, alias);
    if let Some(source) = &filter.source {
        sql.push_str(&format!(" AND {alias}.source = ?"));
        params.push(source.clone());
    }
    if let Some(project) = &filter.project {
        sql.push_str(&format!(" AND {alias}.project LIKE ?"));
        params.push(format!("%{project}%"));
    }
    if let Some(tag) = &filter.tag {
        sql.push_str(&format!(" AND {}", tag_filter_clause(alias)));
        params.push(normalize_tag_name(tag));
    }
    match role {
        SearchRole::All => {}
        SearchRole::User => sql.push_str(&format!(" AND {alias}.role = 'user'")),
        SearchRole::Assistant => sql.push_str(&format!(" AND {alias}.role = 'assistant'")),
    }
}

/// Restrict a history/event query by where its canonical session is present.
///
/// Legacy rows with no session id or no classification are local so upgrading
/// cannot make existing history disappear. Remote requires an explicit remote
/// presence. `all` adds no predicate and therefore cannot multiply rows.
fn append_session_scope_filter(sql: &mut String, scope: SessionScope, alias: &str) {
    match scope {
        SessionScope::Local => sql.push_str(&format!(
            " AND ({alias}.session_id IS NULL \
               OR EXISTS (SELECT 1 FROM session_presences p WHERE p.source = {alias}.source AND p.session_id = {alias}.session_id AND p.location = 'local') \
               OR NOT EXISTS (SELECT 1 FROM session_presences p WHERE p.source = {alias}.source AND p.session_id = {alias}.session_id))"
        )),
        SessionScope::Remote => sql.push_str(&format!(
            " AND {alias}.session_id IS NOT NULL \
               AND EXISTS (SELECT 1 FROM session_presences p WHERE p.source = {alias}.source AND p.session_id = {alias}.session_id AND p.location = 'remote')"
        )),
        SessionScope::All => {}
    }
}

fn tag_filter_clause(alias: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM session_tags st JOIN tags t ON t.id = st.tag_id WHERE st.source = {alias}.source AND st.session_id = {alias}.session_id AND t.name = ?)"
    )
}
