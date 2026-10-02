//! Typed history and event search, independent of command-line formatting.
//!
//! This is the one search contract every surface uses: the CLI's `search`,
//! the napi `search` the TypeScript SDK wraps, and the MCP `search_history`
//! tool. It matches user prompts (`history`) and normalized session events
//! (`session_events`) through their FTS5 indexes, applies the same
//! scope/source/project/tag/time filters to both, and orders the merged result
//! by `(timestamp_ms DESC, id DESC, match_source)` so a tie never reorders
//! between calls.
use crate::{
    append_window_filters, normalize_tag_name, raw_fts_query_error, HistoryCursor, HistoryPage,
    QueryFilter, SessionScope,
};
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
/// Which rows a search may match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchRole {
    /// Prompts and every session event.
    All,
    /// Prompts and user-role session events.
    User,
    /// Assistant-role session events only.
    Assistant,
    /// Prompts (`history` rows) only — what `resume` and `pack` search.
    Prompt,
}

impl SearchRole {
    /// Parse the wire spelling every surface accepts: `all`, `user`,
    /// `assistant`, `prompt`.
    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "all" => Ok(Self::All),
            "user" => Ok(Self::User),
            "assistant" => Ok(Self::Assistant),
            "prompt" => Ok(Self::Prompt),
            other => anyhow::bail!(
                "search role must be one of all, user, assistant, prompt (got {other})"
            ),
        }
    }
}

/// Where a search match was found.
pub const MATCH_SOURCE_HISTORY: &str = "history";
pub const MATCH_SOURCE_SESSION_EVENT: &str = "session_event";

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
    filter.validate()?;
    let mut rows = Vec::new();
    if !matches!(role, SearchRole::Assistant) {
        rows.extend(search_history_rows(conn, terms, raw_fts, filter)?);
    }
    if !matches!(role, SearchRole::Prompt) {
        rows.extend(search_event_rows(conn, terms, raw_fts, filter, role)?);
    }
    rows.sort_by(|a, b| {
        b.timestamp_ms
            .cmp(&a.timestamp_ms)
            .then_with(|| b.id.cmp(&a.id))
            .then_with(|| a.match_source.cmp(&b.match_source))
    });
    rows.truncate(filter.limit.max(1) as usize);
    Ok(rows)
}

/// One page of [`search_all`], with a cursor to the next page when a further
/// match exists. The cursor carries the last row's match source, because a
/// prompt and an event can share `(timestamp_ms, id)`.
pub fn search_page(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
) -> Result<HistoryPage<SearchRow>> {
    // One row past the page shows whether another follows; a limit no table
    // can reach is clamped so that over-fetch cannot overflow.
    let limit = filter.limit.clamp(1, i64::MAX - 1);
    let mut rows = search_all(
        conn,
        terms,
        raw_fts,
        &QueryFilter {
            limit: limit + 1,
            ..filter.clone()
        },
        role,
    )?;
    let next_cursor = if rows.len() as i64 > limit {
        rows.truncate(limit as usize);
        rows.last().map(|row| HistoryCursor {
            timestamp_ms: row.timestamp_ms,
            id: row.id,
            match_source: Some(row.match_source.clone()),
        })
    } else {
        None
    };
    Ok(HistoryPage { rows, next_cursor })
}

fn search_history_rows(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
) -> Result<Vec<SearchRow>> {
    let fts_match = (!terms.is_empty()).then(|| {
        let query = crate::build_fts_query(terms, raw_fts);
        BranchMatch {
            probe: query.clone(),
            scan: query,
        }
    });
    let mut filter_sql = String::new();
    let mut filter_params = Vec::new();
    append_history_search_filters(&mut filter_sql, &mut filter_params, filter, "h");
    search_branch(
        conn,
        &HISTORY_BRANCH,
        fts_match,
        &filter_sql,
        filter_params,
        filter,
        raw_fts,
    )
}

fn search_event_rows(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
) -> Result<Vec<SearchRow>> {
    // `session_events_fts` also indexes `role`; an ordinary query must not
    // match `assistant` or `user` against it, or searching for the word would
    // return every event of that role. `text` and `project` are the columns
    // `history_fts` matches a prompt on (`prompt`, `project`), so both
    // branches read a term the same way. A raw query is the caller's FTS5
    // expression, column filters included, and is left as written.
    //
    // Reading through the index, a role search also matches the role column,
    // so the index hands back only that role's events instead of every event
    // the term matches; the SQL role predicate still decides. A walk tests
    // one event at a time, where that extra column costs more than the role
    // predicate it would duplicate, so it tests the term alone.
    let fts_match = (!terms.is_empty()).then(|| {
        let query = crate::build_fts_query(terms, raw_fts);
        if raw_fts {
            return BranchMatch {
                probe: query.clone(),
                scan: query,
            };
        }
        let probe = format!("{{text project}} : ({query})");
        let scan = match role {
            SearchRole::User => format!("{probe} AND role : user"),
            SearchRole::Assistant => format!("{probe} AND role : assistant"),
            SearchRole::All | SearchRole::Prompt => probe.clone(),
        };
        BranchMatch { probe, scan }
    });
    // The query the history branch runs, which a prompt must match for its
    // event copy to be dropped as a duplicate of it.
    let prompt_match = (!terms.is_empty()).then(|| crate::build_fts_query(terms, raw_fts));
    let mut filter_sql = String::new();
    let mut filter_params = Vec::new();
    append_event_search_filters(
        &mut filter_sql,
        &mut filter_params,
        filter,
        "e",
        role,
        prompt_match.as_deref(),
    );
    search_branch(
        conn,
        &EVENT_BRANCH,
        fts_match,
        &filter_sql,
        filter_params,
        filter,
        raw_fts,
    )
}

/// A branch's FTS5 MATCH expression, in the two forms its plans use.
struct BranchMatch {
    /// Tests one row by rowid, in the timestamp walk.
    probe: String,
    /// Reads every match through the index: the match count, and the
    /// index-driven plan.
    scan: String,
}

/// One searchable table: its rows, their FTS5 index, and the timestamp index
/// a newest-first walk reads.
struct SearchBranch {
    table: &'static str,
    alias: &'static str,
    fts: &'static str,
    ts_column: &'static str,
    ts_index: &'static str,
    columns: &'static str,
    /// Whether a row sorts after a `history` row with the same
    /// `(timestamp, id)`; see [`append_window_filters`].
    after_history_tie: bool,
    row: fn(&rusqlite::Row<'_>) -> rusqlite::Result<SearchRow>,
}

const HISTORY_BRANCH: SearchBranch = SearchBranch {
    table: "history",
    alias: "h",
    fts: "history_fts",
    ts_column: "timestamp_ms",
    ts_index: "idx_history_timestamp",
    columns: "h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms",
    after_history_tie: false,
    row: |row| {
        Ok(SearchRow {
            id: row.get(0)?,
            source: row.get(1)?,
            session_id: row.get(2)?,
            project: row.get(3)?,
            text: row.get(4)?,
            timestamp_ms: row.get(5)?,
            role: "user".to_string(),
            kind: "history".to_string(),
            match_source: MATCH_SOURCE_HISTORY.to_string(),
        })
    },
};

const EVENT_BRANCH: SearchBranch = SearchBranch {
    table: "session_events",
    alias: "e",
    fts: "session_events_fts",
    ts_column: "ts_ms",
    ts_index: "idx_session_events_ts",
    columns: "e.id, e.source, e.session_id, e.project, COALESCE(e.text, ''), e.ts_ms, e.role, e.kind",
    after_history_tie: true,
    row: |row| {
        Ok(SearchRow {
            id: row.get(0)?,
            source: row.get(1)?,
            session_id: row.get(2)?,
            project: row.get(3)?,
            text: row.get(4)?,
            timestamp_ms: row.get(5)?,
            role: row.get(6)?,
            kind: row.get(7)?,
            match_source: MATCH_SOURCE_SESSION_EVENT.to_string(),
        })
    },
};

/// A query matching fewer rows than this is read through its FTS5 index and
/// sorted: the work is bounded by the match count. Counting up to it reads
/// only the head of the term's doclist. Tests shrink this bound and
/// [`RECENT_WINDOW`] so small fixtures cross both.
const MATCH_SORT_CAP: i64 = if cfg!(test) { 3 } else { 5_000 };

/// How many of the newest rows a search reads in timestamp order before it
/// falls back to sorting every match. See [`search_branch`].
const RECENT_WINDOW: i64 = if cfg!(test) { 6 } else { 20_000 };

/// The first, smaller window a walk reads. When too few of its rows match to
/// expect [`RECENT_WINDOW`] to fill the page, the search falls back without
/// reading the rest.
const PROBE_WINDOW: i64 = if cfg!(test) { 3 } else { 2_000 };

#[cfg(test)]
thread_local! {
    /// Set by tests to read every search through the index-driven plan, the
    /// reference the walk must agree with.
    static SORT_EVERY_MATCH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// How many walks returned their rows, and how many fell back.
    static WALK_OUTCOMES: std::cell::Cell<(u32, u32)> = const { std::cell::Cell::new((0, 0)) };
}

#[cfg(test)]
fn record_walk(served: bool) {
    WALK_OUTCOMES.with(|outcomes| {
        let (hits, misses) = outcomes.get();
        outcomes.set(if served {
            (hits + 1, misses)
        } else {
            (hits, misses + 1)
        });
    });
}

#[cfg(not(test))]
fn record_walk(_served: bool) {}

fn walk_allowed() -> bool {
    #[cfg(test)]
    if SORT_EVERY_MATCH.with(|sort| sort.get()) {
        return false;
    }
    true
}

/// One branch's matches, newest first: `filter_sql` and `filter_params` are
/// the branch's predicates on its alias, `fts_match` its MATCH expression.
///
/// A common term matches most of a table, and joining the FTS5 index to it
/// then reads and sorts every match to return the top `limit`: hundreds of
/// milliseconds on a few hundred thousand events. So a search walks the
/// timestamp index instead, testing each row against the FTS5 index by rowid
/// and stopping at `limit`.
///
/// The walk is bounded to the newest [`RECENT_WINDOW`] rows that pass the
/// time window and cursor, read after a [`PROBE_WINDOW`] probe. Those rows are
/// a prefix of the result order, so when `limit` matches are found among them
/// they are exactly the first `limit` of the full search; otherwise (a term
/// only older sessions use, or a filter few rows pass) the search falls back
/// to the index-driven plan. A query with few matches skips the walk: sorting
/// them is already cheap.
fn search_branch(
    conn: &Connection,
    branch: &SearchBranch,
    fts_match: Option<BranchMatch>,
    filter_sql: &str,
    filter_params: Vec<String>,
    filter: &QueryFilter,
    raw_fts: bool,
) -> Result<Vec<SearchRow>> {
    let SearchBranch {
        table,
        alias,
        fts,
        ts_column,
        ts_index,
        columns,
        ..
    } = branch;
    let limit = filter.limit.max(1);
    // A window smaller than the page could never fill it.
    let mut walk = walk_allowed() && limit < RECENT_WINDOW;
    if walk {
        if let Some(query) = &fts_match {
            let matches: i64 = conn
                .prepare_cached(&format!(
                    "SELECT count(*) FROM (SELECT 1 FROM {fts} WHERE {fts} MATCH ? LIMIT ?)"
                ))?
                .query_row(params![query.scan, MATCH_SORT_CAP], |row| row.get(0))
                .map_err(|error| raw_fts_query_error(raw_fts, error))?;
            walk = matches >= MATCH_SORT_CAP;
        }
    }
    if walk && index_exists(conn, ts_index)? {
        let walk = |window| {
            walk_window(
                conn,
                branch,
                fts_match.as_ref(),
                filter_sql,
                &filter_params,
                filter,
                raw_fts,
                window,
            )
        };
        // A window that holds every eligible row is the whole search.
        let (rows, bounded) = walk(PROBE_WINDOW)?;
        let filled = !bounded || rows.len() as i64 >= limit;
        // Read the full window only when the probe found matches at a rate
        // that could fill the page there (with 4x slack, as matches cluster
        // by session): a filter nothing recent passes, or a term the newest
        // rows do not use, costs the small probe instead of the whole window.
        let promising =
            !rows.is_empty() && rows.len() as i64 * (RECENT_WINDOW / PROBE_WINDOW) * 4 >= limit;
        if filled {
            record_walk(true);
            return Ok(rows);
        }
        if promising {
            let (rows, bounded) = walk(RECENT_WINDOW)?;
            let filled = !bounded || rows.len() as i64 >= limit;
            record_walk(filled);
            if filled {
                return Ok(rows);
            }
        } else {
            record_walk(false);
        }
    }

    let mut params_vec = Vec::new();
    let mut sql = match &fts_match {
        None => format!("SELECT {columns} FROM {table} {alias} WHERE 1=1"),
        Some(query) => {
            params_vec.push(query.scan.clone());
            format!(
                "SELECT {columns} FROM {fts} f JOIN {table} {alias} ON f.rowid = {alias}.id \
                 WHERE {fts} MATCH ?"
            )
        }
    };
    sql.push_str(filter_sql);
    params_vec.extend(filter_params);
    sql.push_str(&format!(
        " ORDER BY {alias}.{ts_column} DESC, {alias}.id DESC LIMIT ?"
    ));
    params_vec.push(limit.to_string());
    query_branch_rows(conn, branch, &sql, params_vec, raw_fts)
}

/// Whether an index exists. `init_db` creates both timestamp indexes, but
/// neither is a schema-currency requirement, so a read-only handle on an
/// older database may lack one and `INDEXED BY` would fail to prepare.
fn index_exists(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn
        .prepare_cached("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?")?
        .exists([name])?)
}

/// The branch's matches among the newest `window` rows that pass the time
/// window and cursor, at most `limit` of them, and whether that window was
/// bounded (`false` when fewer rows pass, so the walk read every one).
///
/// The window is an exact prefix of the `(timestamp DESC, id DESC)` order:
/// it ends at the `window`-th row itself, tie-break included, so `limit`
/// matches found in it are the search's first `limit`.
fn walk_window(
    conn: &Connection,
    branch: &SearchBranch,
    fts_match: Option<&BranchMatch>,
    filter_sql: &str,
    filter_params: &[String],
    filter: &QueryFilter,
    raw_fts: bool,
    window: i64,
) -> Result<(Vec<SearchRow>, bool)> {
    let SearchBranch {
        table,
        alias,
        fts,
        ts_column,
        ts_index,
        columns,
        ..
    } = branch;
    let floor = recent_window_floor(conn, branch, filter, window)?;
    let mut sql = format!("SELECT {columns} FROM {table} {alias} INDEXED BY {ts_index} WHERE 1=1");
    let mut params_vec = Vec::new();
    if let Some(query) = fts_match {
        sql.push_str(&format!(
            " AND EXISTS (SELECT 1 FROM {fts} f WHERE {fts} MATCH ? AND f.rowid = {alias}.id)"
        ));
        params_vec.push(query.probe.clone());
    }
    if let Some((floor_ts, floor_id)) = floor {
        sql.push_str(&format!(
            " AND {alias}.{ts_column} >= ? AND ({alias}.{ts_column} > ? OR {alias}.id >= ?)"
        ));
        params_vec.push(floor_ts.to_string());
        params_vec.push(floor_ts.to_string());
        params_vec.push(floor_id.to_string());
    }
    sql.push_str(filter_sql);
    params_vec.extend(filter_params.iter().cloned());
    sql.push_str(&format!(
        " ORDER BY {alias}.{ts_column} DESC, {alias}.id DESC LIMIT ?"
    ));
    params_vec.push(filter.limit.max(1).to_string());
    let rows = query_branch_rows(conn, branch, &sql, params_vec, raw_fts)?;
    Ok((rows, floor.is_some()))
}

/// The `(timestamp, id)` of the `window`-th newest row that passes the
/// search's time window and cursor, or `None` when fewer rows pass.
fn recent_window_floor(
    conn: &Connection,
    branch: &SearchBranch,
    filter: &QueryFilter,
    window: i64,
) -> Result<Option<(i64, i64)>> {
    let SearchBranch {
        table,
        alias,
        ts_column,
        ts_index,
        after_history_tie,
        ..
    } = branch;
    let ts = format!("{alias}.{ts_column}");
    let mut sql =
        format!("SELECT {ts}, {alias}.id FROM {table} {alias} INDEXED BY {ts_index} WHERE 1=1");
    let mut params_vec = Vec::new();
    if let Some(before_ms) = filter.before_ms {
        sql.push_str(&format!(" AND {ts} < ?"));
        params_vec.push(before_ms.to_string());
    }
    append_window_filters(
        &mut sql,
        &mut params_vec,
        filter,
        &ts,
        &format!("{alias}.id"),
        *after_history_tie,
    );
    sql.push_str(&format!(
        " ORDER BY {ts} DESC, {alias}.id DESC LIMIT 1 OFFSET ?"
    ));
    params_vec.push((window - 1).to_string());
    Ok(conn
        .prepare(&sql)?
        .query_row(rusqlite::params_from_iter(params_vec), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()?)
}

fn query_branch_rows(
    conn: &Connection,
    branch: &SearchBranch,
    sql: &str,
    params_vec: Vec<String>,
    raw_fts: bool,
) -> Result<Vec<SearchRow>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_vec), branch.row)
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
    if let Some(before_ms) = filter.before_ms {
        sql.push_str(&format!(" AND {alias}.timestamp_ms < ?"));
        params.push(before_ms.to_string());
    }
    append_window_filters(
        sql,
        params,
        filter,
        &format!("{alias}.timestamp_ms"),
        &format!("{alias}.id"),
        false,
    );
}

/// The characters Rust's `str::trim` strips (Unicode `White_Space`), as a
/// SQLite `trim` set, so a prompt a parser trimmed in Rust pairs with its
/// verbatim event text.
const RUST_WHITESPACE: &str = "char(9, 10, 11, 12, 13, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, \
     8196, 8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288)";

fn append_event_search_filters(
    sql: &mut String,
    params: &mut Vec<String>,
    filter: &QueryFilter,
    alias: &str,
    role: SearchRole,
    prompt_match: Option<&str>,
) {
    append_session_scope_filter(sql, filter.scope, alias);
    // A hydrated session records each prompt twice: as its `history` row and
    // as a user text event, written from the same provider record with the
    // same timestamp. The prompt is the `history` match; the event copy would
    // only repeat it under a second id. A copy is that turn's row -- same
    // session, same timestamp, same text up to the surrounding whitespace a
    // parser trims before writing `history` (OpenCode, with `str::trim`) --
    // not merely equal text, so a later turn that repeats an earlier prompt
    // still matches. It is dropped only when the prompt is itself one of this
    // search's `history` matches: it matches the same query and passes the
    // same filters. Scope, source and tag are per session and time is the
    // shared timestamp, so the query and the per-row project are re-applied.
    // An assistant search reads no prompts and no user events, so it has no
    // copies to drop.
    if role != SearchRole::Assistant {
        let mut mirror = format!(
            "SELECT 1 FROM history hp WHERE hp.source = {alias}.source \
               AND hp.session_id = {alias}.session_id AND hp.timestamp_ms = {alias}.ts_ms \
               AND hp.prompt = trim({alias}.text, {RUST_WHITESPACE})"
        );
        if let Some(query) = prompt_match {
            mirror.push_str(
                " AND hp.id IN (SELECT rowid FROM history_fts WHERE history_fts MATCH ?)",
            );
            params.push(query.to_string());
        }
        if let Some(project) = &filter.project {
            mirror.push_str(" AND hp.project LIKE ?");
            params.push(format!("%{project}%"));
        }
        sql.push_str(&format!(
            " AND NOT ({alias}.role = 'user' AND {alias}.kind = 'text' AND EXISTS ({mirror}))"
        ));
    }
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
    if let Some(before_ms) = filter.before_ms {
        sql.push_str(&format!(" AND {alias}.ts_ms < ?"));
        params.push(before_ms.to_string());
    }
    append_window_filters(
        sql,
        params,
        filter,
        &format!("{alias}.ts_ms"),
        &format!("{alias}.id"),
        true,
    );
    match role {
        SearchRole::All | SearchRole::Prompt => {}
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{init_db, insert_history, HistoryEntry};
    use rusqlite::params;

    fn fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // Three prompts and three events share one timestamp, so only the id
        // tie-break can order them; a fourth prompt is older.
        for (index, ts) in [(1, 1_000), (2, 1_000), (3, 1_000), (4, 500)] {
            insert_history(
                &conn,
                &HistoryEntry {
                    id: 0,
                    source: "claude".into(),
                    session_id: Some("s1".into()),
                    project: Some("/work/needle".into()),
                    prompt: format!("needle prompt {index}"),
                    prompt_hash: Some(format!("hash-{index}")),
                    timestamp_ms: ts,
                },
            )
            .unwrap();
        }
        for (uid, role, kind) in [
            ("e1", "assistant", "text"),
            ("e2", "tool_result", "tool_result"),
            ("e3", "assistant", "text"),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', 'm1', 1000, ?, ?, ?, ?)",
                params![role, kind, format!("needle event {uid}"), uid],
            )
            .unwrap();
        }
        conn
    }

    fn filter(limit: i64) -> QueryFilter {
        QueryFilter {
            limit,
            ..Default::default()
        }
    }

    fn keys(rows: &[SearchRow]) -> Vec<(String, i64)> {
        rows.iter()
            .map(|row| (row.match_source.clone(), row.id))
            .collect()
    }

    #[test]
    fn ties_are_ordered_by_id_then_match_source_and_limit_takes_the_top() {
        let conn = fixture();
        let terms = ["needle".to_string()];
        let all = search_all(&conn, &terms, false, &filter(100), SearchRole::All).unwrap();
        assert_eq!(
            keys(&all),
            vec![
                ("history".into(), 3),
                ("session_event".into(), 3),
                ("history".into(), 2),
                ("session_event".into(), 2),
                ("history".into(), 1),
                ("session_event".into(), 1),
                ("history".into(), 4),
            ]
        );
        // Each branch pushes the limit into SQL; the merged top-N must still
        // be the prefix of the full ordering.
        for limit in 1..=all.len() as i64 {
            let page = search_all(&conn, &terms, false, &filter(limit), SearchRole::All).unwrap();
            assert_eq!(keys(&page), keys(&all)[..limit as usize].to_vec());
        }
    }

    #[test]
    fn roles_select_match_sources_and_carry_provenance() {
        let conn = fixture();
        let terms = ["needle".to_string()];
        let assistant =
            search_all(&conn, &terms, false, &filter(100), SearchRole::Assistant).unwrap();
        assert_eq!(
            keys(&assistant),
            vec![("session_event".into(), 3), ("session_event".into(), 1)]
        );
        assert!(assistant
            .iter()
            .all(|row| row.role == "assistant" && row.kind == "text"));

        let user = search_all(&conn, &terms, false, &filter(100), SearchRole::User).unwrap();
        assert_eq!(user.len(), 4);
        assert!(user.iter().all(|row| row.match_source == "history"
            && row.role == "user"
            && row.kind == "history"));

        let prompts = search_all(&conn, &terms, false, &filter(100), SearchRole::Prompt).unwrap();
        assert_eq!(
            keys(&prompts),
            vec![
                ("history".into(), 3),
                ("history".into(), 2),
                ("history".into(), 1),
                ("history".into(), 4),
            ]
        );
    }

    #[test]
    fn a_prompt_mirrored_as_a_user_event_matches_once_as_history() {
        let conn = fixture();
        // `u-copy` is prompt 1's own event; `u-padded` is prompt 2's, with
        // the whitespace a parser trims before writing `history`.
        for (uid, ts, text) in [
            ("u-copy", 1_000, "needle prompt 1"),
            ("u-padded", 1_000, "\u{a0} needle prompt 2\n\u{3000}"),
            ("u-own", 2_000, "needle follow-up"),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', 'm2', ?, 'user', 'text', ?, ?)",
                params![ts, text, uid],
            )
            .unwrap();
        }
        let rows = search_all(
            &conn,
            &["needle".to_string()],
            false,
            &filter(100),
            SearchRole::User,
        )
        .unwrap();
        let events = rows
            .iter()
            .filter(|row| row.match_source == MATCH_SOURCE_SESSION_EVENT)
            .map(|row| row.text.as_str())
            .collect::<Vec<_>>();
        assert_eq!(events, vec!["needle follow-up"]);
        assert_eq!(
            rows.iter()
                .filter(|row| row.text == "needle prompt 1")
                .count(),
            1
        );
    }

    fn prompt_and_user_event(prompt_project: &str, event_ts: i64) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some(prompt_project.into()),
                prompt: "retry please".into(),
                prompt_hash: Some("hash-retry".into()),
                timestamp_ms: 1_000,
            },
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm1', ?, 'user', 'text', 'retry please', 'u1')",
            [event_ts],
        )
        .unwrap();
        conn
    }

    fn filtered(before_ms: Option<i64>, project: Option<&str>) -> QueryFilter {
        QueryFilter {
            before_ms,
            project: project.map(str::to_string),
            limit: 100,
            ..Default::default()
        }
    }

    #[test]
    fn a_later_turn_repeating_a_prompt_is_not_taken_for_its_copy() {
        let conn = prompt_and_user_event("/work/needle", 2_000);
        let terms = ["retry".to_string()];
        let all = search_all(&conn, &terms, false, &filtered(None, None), SearchRole::All).unwrap();
        assert_eq!(
            keys(&all),
            vec![("session_event".into(), 1), ("history".into(), 1)]
        );
        let before = search_all(
            &conn,
            &terms,
            false,
            &filtered(Some(1_500), None),
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(keys(&before), vec![("history".into(), 1)]);
    }

    #[test]
    fn a_copy_survives_when_its_prompt_fails_the_project_filter() {
        // The prompt and its event copy share the timestamp, but the prompt
        // row carries a different project.
        let conn = prompt_and_user_event("/work/other", 1_000);
        let terms = ["retry".to_string()];
        let all = search_all(&conn, &terms, false, &filtered(None, None), SearchRole::All).unwrap();
        assert_eq!(keys(&all), vec![("history".into(), 1)]);
        let project = search_all(
            &conn,
            &terms,
            false,
            &filtered(None, Some("needle")),
            SearchRole::User,
        )
        .unwrap();
        assert_eq!(keys(&project), vec![("session_event".into(), 1)]);
    }

    #[test]
    fn a_copy_matched_only_by_its_own_project_is_kept() {
        // Same turn, but only the event's project matches the query; the
        // prompt is not a match, so the event must not be dropped for it.
        let conn = prompt_and_user_event("/work/alpha", 1_000);
        let by_text =
            search_all(&conn, &["retry".to_string()], false, &filtered(None, None), SearchRole::All)
                .unwrap();
        assert_eq!(keys(&by_text), vec![("history".into(), 1)]);
        let by_project = search_all(
            &conn,
            &["needle".to_string()],
            false,
            &filtered(None, None),
            SearchRole::User,
        )
        .unwrap();
        assert_eq!(keys(&by_project), vec![("session_event".into(), 1)]);
    }

    #[test]
    fn an_ordinary_query_does_not_match_an_events_role() {
        let conn = fixture();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm3', 3000, 'assistant', 'text', 'done', 'e-done')",
            [],
        )
        .unwrap();
        let role_word = ["assistant".to_string()];
        assert!(search_all(&conn, &role_word, false, &filter(100), SearchRole::All)
            .unwrap()
            .is_empty());
        // The project column is matched on both branches, as `history_fts`
        // always has; a raw query may still name the role column itself.
        let project_word = ["work".to_string()];
        let by_project =
            search_all(&conn, &project_word, false, &filter(100), SearchRole::Assistant).unwrap();
        assert!(by_project.iter().any(|row| row.text == "done"));
        let raw = ["role : assistant".to_string()];
        let by_role =
            search_all(&conn, &raw, true, &filter(100), SearchRole::Assistant).unwrap();
        assert_eq!(by_role.len(), 3);
    }

    #[test]
    fn before_ms_bounds_both_prompts_and_events() {
        let conn = fixture();
        let rows = search_all(
            &conn,
            &["needle".to_string()],
            false,
            &QueryFilter {
                before_ms: Some(1_000),
                limit: 100,
                ..Default::default()
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(keys(&rows), vec![("history".into(), 4)]);
    }

    fn walk(conn: &Connection, page_size: i64, role: SearchRole) -> Vec<(String, i64)> {
        let mut seen = Vec::new();
        let mut after = None;
        loop {
            let page = search_page(
                conn,
                &["needle".to_string()],
                false,
                &QueryFilter {
                    after: after.clone(),
                    ..filter(page_size)
                },
                role,
            )
            .unwrap();
            assert!(page.rows.len() as i64 <= page_size);
            seen.extend(keys(&page.rows));
            match page.next_cursor {
                Some(cursor) => after = Some(cursor),
                None => return seen,
            }
        }
    }

    #[test]
    fn keyset_pages_traverse_tied_matches_exactly_once() {
        let conn = fixture();
        let full = keys(
            &search_all(
                &conn,
                &["needle".to_string()],
                false,
                &filter(100),
                SearchRole::All,
            )
            .unwrap(),
        );
        assert_eq!(full.len(), 7);
        for page_size in [1, 2, 3, 7, 50] {
            assert_eq!(walk(&conn, page_size, SearchRole::All), full, "page size {page_size}");
        }
        assert_eq!(walk(&conn, 1, SearchRole::Prompt).len(), 4);

        // The legacy exclusive timestamp skips the rest of a tie.
        let first = search_all(
            &conn,
            &["needle".to_string()],
            false,
            &filter(1),
            SearchRole::All,
        )
        .unwrap();
        let legacy = search_all(
            &conn,
            &["needle".to_string()],
            false,
            &QueryFilter {
                before_ms: Some(first[0].timestamp_ms),
                ..filter(100)
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(legacy.len(), 1, "before_ms drops the five tied rows");
    }

    #[test]
    fn since_and_until_are_inclusive_and_validated() {
        let conn = fixture();
        let window = |since: Option<i64>, until: Option<i64>| {
            search_all(
                &conn,
                &["needle".to_string()],
                false,
                &QueryFilter {
                    since_ms: since,
                    until_ms: until,
                    ..filter(100)
                },
                SearchRole::All,
            )
        };
        assert_eq!(window(Some(1_000), None).unwrap().len(), 6);
        assert_eq!(window(None, Some(500)).unwrap().len(), 1);
        assert_eq!(window(Some(500), Some(500)).unwrap().len(), 1);
        assert_eq!(window(Some(500), Some(1_000)).unwrap().len(), 7);
        let error = window(Some(1_001), Some(1_000)).unwrap_err().to_string();
        assert!(error.contains("must not be later than"), "got: {error}");

        let bad_cursor = search_all(
            &conn,
            &["needle".to_string()],
            false,
            &QueryFilter {
                after: Some(HistoryCursor {
                    timestamp_ms: 1,
                    id: 1,
                    match_source: Some("tool".into()),
                }),
                ..filter(100)
            },
            SearchRole::All,
        )
        .unwrap_err()
        .to_string();
        assert!(bad_cursor.contains("match_source"), "got: {bad_cursor}");
    }

    #[test]
    fn recent_pages_traverse_tied_prompts_exactly_once() {
        let conn = fixture();
        for page_size in [1, 2, 4, 10] {
            let mut ids = Vec::new();
            let mut after = None;
            loop {
                let page = crate::recent_page(
                    &conn,
                    &QueryFilter {
                        after: after.clone(),
                        ..filter(page_size)
                    },
                )
                .unwrap();
                ids.extend(page.rows.iter().map(|row| row.id));
                match page.next_cursor {
                    Some(cursor) => after = Some(cursor),
                    None => break,
                }
            }
            assert_eq!(ids, vec![3, 2, 1, 4], "page size {page_size}");
        }
    }

    #[test]
    fn the_largest_limit_returns_every_row_without_overflowing() {
        let conn = fixture();
        let terms = ["needle".to_string()];
        let search = search_page(&conn, &terms, false, &filter(i64::MAX), SearchRole::All).unwrap();
        assert_eq!(search.rows.len(), 7);
        assert!(search.next_cursor.is_none());
        let recent = crate::recent_page(&conn, &filter(i64::MAX)).unwrap();
        assert_eq!(recent.rows.len(), 4);
        assert!(recent.next_cursor.is_none());
    }

    #[test]
    fn role_parse_accepts_the_wire_spellings_only() {
        assert_eq!(SearchRole::parse("all").unwrap(), SearchRole::All);
        assert_eq!(SearchRole::parse("user").unwrap(), SearchRole::User);
        assert_eq!(
            SearchRole::parse("assistant").unwrap(),
            SearchRole::Assistant
        );
        assert_eq!(SearchRole::parse("prompt").unwrap(), SearchRole::Prompt);
        let error = SearchRole::parse("tool").unwrap_err().to_string();
        assert!(error.contains("all, user, assistant, prompt"), "got: {error}");
    }

    /// Sessions across sources and locations, prompts mirrored as user
    /// events, and a term the newest rows rarely use: enough rows that the
    /// test-sized window and match cap are crossed both ways.
    fn walk_fixture() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let sessions = [
            ("claude", "s1", "/work/alpha"),
            ("claude", "s2", "/work/beta"),
            ("codex", "s3", "/work/alpha"),
            ("codex", "s4", "/work/gamma"),
        ];
        for (index, (source, session, _)) in sessions.iter().enumerate() {
            let location = if index == 3 {
                crate::SessionLocation::Remote
            } else {
                crate::SessionLocation::Local
            };
            crate::mark_session_presence(&conn, source, session, location).unwrap();
        }
        conn.execute(
            "INSERT INTO tags (name, display_name, created_ms, updated_ms) VALUES ('hot', 'hot', 0, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_tags (source, session_id, tag_id, created_ms) VALUES ('claude', 's2', 1, 0)",
            [],
        )
        .unwrap();
        let mut seed = 7_u64;
        let mut next = move |bound: u64| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (seed >> 33) % bound
        };
        for index in 0..90_i64 {
            let (source, session, project) = sessions[next(4) as usize];
            // Timestamps collide often, so ties cross the window boundary.
            let ts = 1_000 + (next(40) as i64) * 10;
            // `rare` only in the oldest rows, `needle` everywhere.
            let mut text = format!("needle row {index}");
            if ts < 1_100 {
                text.push_str(" rare");
            }
            match next(5) {
                0 => {
                    insert_history(
                        &conn,
                        &HistoryEntry {
                            id: 0,
                            source: source.into(),
                            session_id: Some(session.into()),
                            project: Some(project.into()),
                            prompt: text.clone(),
                            prompt_hash: None,
                            timestamp_ms: ts,
                        },
                    )
                    .unwrap();
                    // Most prompts are mirrored as their own user event.
                    if next(3) > 0 {
                        conn.execute(
                            "INSERT INTO session_events \
                             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                             VALUES (?, ?, ?, 'm', ?, 'user', 'text', ?, ?)",
                            params![source, session, project, ts, format!(" {text}\n"), format!("p{index}")],
                        )
                        .unwrap();
                    }
                }
                pick => {
                    let (role, kind) = match pick {
                        1 => ("user", "text"),
                        2 => ("tool_result", "tool_result"),
                        _ => ("assistant", "text"),
                    };
                    conn.execute(
                        "INSERT INTO session_events \
                         (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                         VALUES (?, ?, ?, 'm', ?, ?, ?, ?, ?)",
                        params![source, session, project, ts, role, kind, text, format!("e{index}")],
                    )
                    .unwrap();
                }
            }
        }
        conn
    }

    fn sorted_reference<T>(read: impl FnOnce() -> T) -> T {
        SORT_EVERY_MATCH.with(|sort| sort.set(true));
        let result = read();
        SORT_EVERY_MATCH.with(|sort| sort.set(false));
        result
    }

    #[test]
    fn the_recent_window_walk_returns_what_sorting_every_match_returns() {
        let conn = walk_fixture();
        let term_sets: [&[&str]; 4] = [&["needle"], &["rare"], &[], &["needle", "-rare"]];
        let roles = [
            SearchRole::All,
            SearchRole::User,
            SearchRole::Assistant,
            SearchRole::Prompt,
        ];
        let filters = [
            QueryFilter::default(),
            QueryFilter {
                scope: SessionScope::All,
                ..Default::default()
            },
            QueryFilter {
                scope: SessionScope::Remote,
                ..Default::default()
            },
            QueryFilter {
                source: Some("codex".into()),
                ..Default::default()
            },
            QueryFilter {
                project: Some("alpha".into()),
                ..Default::default()
            },
            QueryFilter {
                tag: Some("hot".into()),
                ..Default::default()
            },
            QueryFilter {
                since_ms: Some(1_100),
                until_ms: Some(1_300),
                ..Default::default()
            },
            QueryFilter {
                before_ms: Some(1_200),
                ..Default::default()
            },
        ];
        let mut walked = 0;
        WALK_OUTCOMES.with(|outcomes| outcomes.set((0, 0)));
        for terms in term_sets {
            let terms = terms.iter().map(|term| term.to_string()).collect::<Vec<_>>();
            for role in roles {
                for base in &filters {
                    for limit in 1..=5 {
                        let filter = QueryFilter {
                            limit,
                            ..base.clone()
                        };
                        let expected = sorted_reference(|| {
                            search_all(&conn, &terms, false, &filter, role).unwrap()
                        });
                        let actual = search_all(&conn, &terms, false, &filter, role).unwrap();
                        assert_eq!(
                            keys(&actual),
                            keys(&expected),
                            "terms {terms:?} role {role:?} filter {filter:?}"
                        );
                        walked += 1;
                    }
                    // Keyset pages agree page by page, cursor included.
                    let mut after = None;
                    loop {
                        let filter = QueryFilter {
                            limit: 2,
                            after: after.clone(),
                            ..base.clone()
                        };
                        let expected = sorted_reference(|| {
                            search_page(&conn, &terms, false, &filter, role).unwrap()
                        });
                        let actual = search_page(&conn, &terms, false, &filter, role).unwrap();
                        assert_eq!(keys(&actual.rows), keys(&expected.rows));
                        assert_eq!(actual.next_cursor, expected.next_cursor);
                        match actual.next_cursor {
                            Some(cursor) => after = Some(cursor),
                            None => break,
                        }
                    }
                }
            }
        }
        assert!(walked > 0);
        let (served, fell_back) = WALK_OUTCOMES.with(|outcomes| outcomes.get());
        assert!(served > 0 && fell_back > 0, "served {served}, fell back {fell_back}");
    }

    #[test]
    fn a_raw_query_error_is_reported_by_the_walk_too() {
        let conn = walk_fixture();
        let error = search_all(
            &conn,
            &["\"unterminated".to_string()],
            true,
            &filter(2),
            SearchRole::All,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("Invalid raw FTS5 MATCH expression"), "got: {error}");
    }
}
