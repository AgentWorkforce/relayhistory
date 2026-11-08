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
use rusqlite::Connection;
#[cfg(test)]
use walk::{SORT_EVERY_MATCH, WALK_OUTCOMES};
use walk::{walk, walk_pays, BranchSearch};

mod walk;

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

/// Reference to the stored row a match came from: enough for a caller to
/// re-read the full evidence with `get_session_events`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchEventRef {
    pub match_source: String,
    pub id: i64,
    pub session_id: Option<String>,
    pub event_uid: Option<String>,
}

/// The origin of a match, derived — never guessed — from the stored row.
/// Only a `session_events` row with no `control_kind` in the user role is
/// `human`; `peer` is reserved and no stored row maps to it yet.
pub fn derive_origin(match_source: &str, role: &str, control_kind: Option<&str>) -> String {
    if match_source == MATCH_SOURCE_HISTORY {
        return "human".to_string();
    }
    match control_kind {
        None if role == "user" => "human".to_string(),
        Some("synthetic") => "synthetic".to_string(),
        Some("system_reminder") | Some("task_notification") => "system".to_string(),
        _ => "unknown".to_string(),
    }
}

/// Default snippet window in codepoints; see [`QueryFilter::max_content_chars`].
pub const DEFAULT_SNIPPET_CHARS: usize = 320;

/// The literal terms a snippet centers on: the query split on whitespace,
/// stripped of FTS operators, quotes and a trailing `*`. Negated terms and
/// column filters name no literal text, so they never center a snippet.
fn snippet_terms(terms: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for raw in terms.iter().flat_map(|term| term.split_whitespace()) {
        let mut token = raw.trim().trim_matches(|c| c == '"' || c == '\'' || c == '(' || c == ')');
        if token.is_empty() || token.starts_with('-') || token.contains(':') {
            continue;
        }
        if let Some(stripped) = token.strip_suffix('*') {
            token = stripped;
        }
        if token.is_empty() {
            continue;
        }
        let upper = token.to_ascii_uppercase();
        if upper == "AND" || upper == "OR" || upper == "NOT" {
            continue;
        }
        out.push(token.to_string());
    }
    out
}

/// A match-centered window of `text`, at most `max_content_chars` codepoints
/// with any ellipsis inside that cap. Returns the snippet, whether the full
/// text was truncated, and the codepoint offset of the snippet's first real
/// character in `text` (`None` only for empty text). When no literal term
/// occurs — a project-only match, an empty query, a negation or raw FTS with
/// no literal hit — the window is the head of the text at offset 0.
///
/// This is computed here, the lowest layer that still holds the full text:
/// `SearchRow.text` (and so the `prompt` every surface returns, and what
/// `store::search` feeds `resume`/`pack`) always keeps the full text, and
/// the snippet is an additive field beside it.
pub fn compute_snippet(
    text: &str,
    terms: &[String],
    max_content_chars: usize,
) -> (String, bool, Option<usize>) {
    let max = max_content_chars.max(1);
    let chars: Vec<char> = text.chars().collect();
    let total = chars.len();
    if total == 0 {
        return (String::new(), false, None);
    }
    if total <= max {
        return (text.to_string(), false, Some(0));
    }
    let text_lower = text.to_lowercase();
    let mut best: Option<(usize, usize)> = None;
    for term in snippet_terms(terms) {
        let term_lower = term.to_lowercase();
        if term_lower.is_empty() {
            continue;
        }
        if let Some(byte_idx) = text_lower.find(&term_lower) {
            let start = text_lower[..byte_idx].chars().count();
            let len = term_lower.chars().count();
            let better = match best {
                None => true,
                Some((best_start, best_len)) => {
                    start < best_start || (start == best_start && len > best_len)
                }
            };
            if better {
                best = Some((start, len));
            }
        }
    }
    let Some((match_start, _)) = best else {
        let mut snippet: String = chars[..max - 1].iter().collect();
        snippet.push('\u{2026}');
        return (snippet, true, Some(0));
    };
    // 40% of the window before the match start, 60% from it onward.
    let mut start = match_start.saturating_sub(max * 2 / 5);
    if start + max > total {
        start = total - max;
    }
    let end = start + max;
    let head_truncated = start > 0;
    let tail_truncated = end < total;
    let mut snippet = String::new();
    match (head_truncated, tail_truncated) {
        (true, true) => {
            snippet.push('\u{2026}');
            snippet.extend(&chars[start + 1..end - 1]);
            snippet.push('\u{2026}');
            (snippet, true, Some(start + 1))
        }
        (true, false) => {
            snippet.push('\u{2026}');
            snippet.extend(&chars[start + 1..end]);
            (snippet, true, Some(start + 1))
        }
        (false, true) => {
            snippet.extend(&chars[..max - 1]);
            snippet.push('\u{2026}');
            (snippet, true, Some(0))
        }
        (false, false) => (text.to_string(), false, Some(0)),
    }
}

fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Whether `candidate` is linked to `primary` by the transcript's own ids:
/// its `parent_id` or `tool_use_id` names the primary's message or event.
fn linked_to(candidate: &SearchRow, primary: &SearchRow) -> bool {
    let primary_ids = [
        primary.message_id.as_deref(),
        primary.event_uid.as_deref(),
        primary.tool_use_id.as_deref(),
    ];
    let names_primary = |value: &str| {
        value == primary.id.to_string()
            || primary_ids.iter().flatten().any(|id| *id == value)
    };
    candidate.parent_id.as_deref().is_some_and(names_primary)
        || candidate.tool_use_id.as_deref().is_some_and(names_primary)
}

/// Collapse fetched rows into groups around primaries, newest first.
///
/// A primary is a `history` row or a `role = user`/`kind = text` event. Any
/// other row joins a primary only in the same source and session when it is
/// linked to the primary by id, or when it is a tool call/result or an
/// assistant row whose whitespace-collapsed text contains the primary's full
/// trimmed text verbatim (case-sensitive). A user or history row never joins
/// another as an echo, so two identical-wording approvals at different
/// timestamps both stay primaries. There is no fuzzy similarity. A row that
/// joins nothing stays as its own singleton group. Only group
/// representatives are returned, with `group_size`/`echo_count` counting the
/// echoes; `bound_hit` marks the oldest group `group_truncated` because the
/// bounded over-fetch may have cut its remaining echoes.
fn group_rows(rows: Vec<SearchRow>, bound_hit: bool) -> Vec<SearchRow> {
    struct Group {
        representative: SearchRow,
        position: usize,
        size: i64,
        absorbable: bool,
    }
    // Primaries are collected first: an echo is usually newer than the
    // prompt it repeats (the assistant replies after the user), so in
    // newest-first order it precedes its primary and a single pass would
    // miss the join.
    let mut groups: Vec<Group> = Vec::new();
    let mut pending: Vec<(usize, SearchRow)> = Vec::new();
    for (position, row) in rows.into_iter().enumerate() {
        if row.is_primary_candidate() {
            groups.push(Group {
                representative: row,
                position,
                size: 1,
                absorbable: true,
            });
        } else {
            pending.push((position, row));
        }
    }
    for (position, row) in pending {
        let joined = groups.iter().position(|group| {
            group.absorbable
                && group.representative.source == row.source
                && group.representative.session_id == row.session_id
                && (linked_to(&row, &group.representative) || {
                    let primary_text = collapse_whitespace(group.representative.text.trim());
                    !primary_text.is_empty()
                        && (row.kind == "tool_use"
                            || row.kind == "tool_result"
                            || row.role == "assistant")
                        && collapse_whitespace(&row.text).contains(&primary_text)
                })
        });
        match joined {
            Some(index) => groups[index].size += 1,
            None => groups.push(Group {
                representative: row,
                position,
                size: 1,
                absorbable: false,
            }),
        }
    }
    groups.sort_by_key(|group| group.position);
    let last = groups.len().saturating_sub(1);
    groups
        .into_iter()
        .enumerate()
        .map(|(index, group)| {
            let mut row = group.representative;
            row.group_size = group.size;
            row.echo_count = group.size - 1;
            row.group_id = (group.size > 1)
                .then(|| format!("{}:{}", row.match_source, row.id));
            row.group_truncated = bound_hit && index == last;
            row.is_primary = true;
            row
        })
        .collect()
}

/// The bounded over-fetch a grouped search reads before collapsing: enough
/// headroom for echoes around the requested primaries, never unbounded. It
/// also stays inside the timestamp walk's recent window, so a grouped search
/// keeps the walk's fast path instead of always sorting every match.
fn bounded_fetch_limit(requested: i64) -> i64 {
    let bound = if requested > 1_000 {
        requested
    } else {
        (requested * 4).min(4_000) + 1
    };
    bound.min(walk::RECENT_WINDOW - 1).max(requested)
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
    pub message_id: Option<String>,
    pub parent_id: Option<String>,
    pub event_uid: Option<String>,
    pub tool_use_id: Option<String>,
    pub control_kind: Option<String>,
    pub agent_id: Option<String>,
    pub snippet: String,
    pub content_truncated: bool,
    pub snippet_start_offset: Option<usize>,
    pub group_id: Option<String>,
    pub group_size: i64,
    pub echo_count: i64,
    pub group_truncated: bool,
    pub is_primary: bool,
    pub origin: String,
}

impl SearchRow {
    pub fn event_ref(&self) -> SearchEventRef {
        SearchEventRef {
            match_source: self.match_source.clone(),
            id: self.id,
            session_id: self.session_id.clone(),
            event_uid: self.event_uid.clone(),
        }
    }

    fn is_primary_candidate(&self) -> bool {
        self.match_source == MATCH_SOURCE_HISTORY
            || (self.role == "user" && self.kind == "text")
    }
}

pub fn search_all(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
) -> Result<Vec<SearchRow>> {
    filter.validate()?;
    let requested = filter.limit.max(1);
    let fetch_limit = bounded_fetch_limit(requested);
    let fetch_filter = QueryFilter {
        limit: fetch_limit,
        ..filter.clone()
    };
    let mut rows = Vec::new();
    let mut bound_hit = false;
    if !matches!(role, SearchRole::Assistant) {
        let branch = search_history_rows(conn, terms, raw_fts, &fetch_filter)?;
        bound_hit |= branch.len() as i64 >= fetch_limit;
        rows.extend(branch);
    }
    if !matches!(role, SearchRole::Prompt) {
        let branch = search_event_rows(conn, terms, raw_fts, &fetch_filter, role)?;
        bound_hit |= branch.len() as i64 >= fetch_limit;
        rows.extend(branch);
    }
    rows.sort_by(|a, b| {
        b.timestamp_ms
            .cmp(&a.timestamp_ms)
            .then_with(|| b.id.cmp(&a.id))
            .then_with(|| a.match_source.cmp(&b.match_source))
    });
    // Grouping happens before the limit: echoes collapse into their primary
    // and only primaries count toward the page, in the primary's position in
    // the global (timestamp_ms DESC, id DESC, match_source ASC) order, so
    // cursors taken from a page stay valid.
    let max_chars = filter
        .max_content_chars
        .map(|value| value as usize)
        .unwrap_or(DEFAULT_SNIPPET_CHARS);
    let mut grouped = group_rows(rows, bound_hit);
    for row in &mut grouped {
        row.origin = derive_origin(&row.match_source, &row.role, row.control_kind.as_deref());
        let (snippet, truncated, offset) = compute_snippet(&row.text, terms, max_chars);
        row.snippet = snippet;
        row.content_truncated = truncated;
        row.snippet_start_offset = offset;
    }
    grouped.truncate(requested as usize);
    Ok(grouped)
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

/// The newest prompt matching a search that names a session (non-empty
/// session id), or `None`: what `resume` needs, in one query.
pub fn latest_prompt_with_session(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
) -> Result<Option<SearchRow>> {
    let filter = QueryFilter {
        limit: 1,
        ..filter.clone()
    };
    filter.validate()?;
    Ok(
        search_history_rows_where(conn, terms, raw_fts, &filter, true)?
            .into_iter()
            .next(),
    )
}

fn search_history_rows(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
) -> Result<Vec<SearchRow>> {
    search_history_rows_where(conn, terms, raw_fts, filter, false)
}

fn search_history_rows_where(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    with_session: bool,
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
    if with_session {
        filter_sql.push_str(" AND h.session_id IS NOT NULL AND h.session_id != ''");
    }
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
    columns: "h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms, 'user', 'history', NULL, NULL, NULL, NULL, NULL, NULL",
    after_history_tie: false,
    row: |row| search_row(row, MATCH_SOURCE_HISTORY),
};

const EVENT_BRANCH: SearchBranch = SearchBranch {
    table: "session_events",
    alias: "e",
    fts: "session_events_fts",
    ts_column: "ts_ms",
    ts_index: "idx_session_events_ts",
    columns: "e.id, e.source, e.session_id, e.project, COALESCE(e.text, ''), e.ts_ms, e.role, e.kind, e.message_id, e.parent_id, e.event_uid, e.tool_use_id, e.control_kind, e.agent_id",
    after_history_tie: true,
    row: |row| search_row(row, MATCH_SOURCE_SESSION_EVENT),
};


/// Map one branch row (the shared 14-column `columns` list) to a
/// [`SearchRow`]. Snippet and grouping fields start at their singleton
/// defaults; `search_all` fills them in after grouping.
fn search_row(row: &rusqlite::Row<'_>, match_source: &str) -> rusqlite::Result<SearchRow> {
    let role: String = row.get(6)?;
    let control_kind: Option<String> = row.get(12)?;
    let origin = derive_origin(match_source, &role, control_kind.as_deref());
    Ok(SearchRow {
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        project: row.get(3)?,
        text: row.get(4)?,
        timestamp_ms: row.get(5)?,
        role,
        kind: row.get(7)?,
        match_source: match_source.to_string(),
        message_id: row.get(8)?,
        parent_id: row.get(9)?,
        event_uid: row.get(10)?,
        tool_use_id: row.get(11)?,
        control_kind,
        agent_id: row.get(13)?,
        snippet: String::new(),
        content_truncated: false,
        snippet_start_offset: None,
        group_id: None,
        group_size: 1,
        echo_count: 0,
        group_truncated: false,
        is_primary: true,
        origin,
    })
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
/// The walk reads the newest rows that pass the time window and cursor in
/// growing [`WALK_WINDOWS`](walk::WALK_WINDOWS), up to [`RECENT_WINDOW`](walk::RECENT_WINDOW) of them. Those rows are
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
        columns,
        ..
    } = branch;
    let limit = filter.limit.max(1);
    if walk_pays(conn, branch, fts_match.as_ref(), limit, raw_fts)? {
        let search = BranchSearch {
            fts_match: fts_match.as_ref(),
            filter_sql,
            filter_params: &filter_params,
            filter,
            raw_fts,
        };
        if let Some(rows) = walk(conn, branch, &search, limit)? {
            return Ok(rows);
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
    append_row_filters(sql, params, filter, alias, "timestamp_ms", false);
    append_search_specific_filters(sql, params, filter, alias, true);
}

/// The `session_id`, `kinds` and `origin` predicates a search adds on top of
/// the shared row filters. On the `history` branch a row is always a `human`
/// prompt of kind `history`, so the other values exclude the branch.
fn append_search_specific_filters(
    sql: &mut String,
    params: &mut Vec<String>,
    filter: &QueryFilter,
    alias: &str,
    is_history: bool,
) {
    if let Some(session_id) = &filter.session_id {
        sql.push_str(&format!(" AND {alias}.session_id = ?"));
        params.push(session_id.clone());
    }
    if !filter.kinds.is_empty() {
        if is_history {
            if !filter.kinds.iter().any(|kind| kind == "history") {
                sql.push_str(" AND 1=0");
            }
        } else {
            let placeholders = vec!["?"; filter.kinds.len()].join(",");
            sql.push_str(&format!(" AND {alias}.kind IN ({placeholders})"));
            params.extend(filter.kinds.iter().cloned());
        }
    }
    if let Some(origin) = &filter.origin {
        if is_history {
            if origin != "human" {
                sql.push_str(" AND 1=0");
            }
        } else {
            match origin.as_str() {
                "human" => sql.push_str(&format!(
                    " AND {alias}.control_kind IS NULL AND {alias}.role = 'user'"
                )),
                "synthetic" => {
                    sql.push_str(&format!(" AND {alias}.control_kind = ?"));
                    params.push("synthetic".to_string());
                }
                "system" => sql.push_str(&format!(
                    " AND {alias}.control_kind IN ('system_reminder','task_notification')"
                )),
                "unknown" => sql.push_str(&format!(
                    " AND NOT (({alias}.control_kind IS NULL AND {alias}.role = 'user') OR {alias}.control_kind IN ('synthetic','system_reminder','task_notification'))"
                )),
                // `peer` is never guessed from a stored row, so it matches none.
                _ => sql.push_str(" AND 1=0"),
            }
        }
    }
}

/// The source, project, tag, time-window and cursor predicates both branches
/// apply, on `alias` with its timestamp in `ts_column`.
fn append_row_filters(
    sql: &mut String,
    params: &mut Vec<String>,
    filter: &QueryFilter,
    alias: &str,
    ts_column: &str,
    after_history_tie: bool,
) {
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
        sql.push_str(&format!(" AND {alias}.{ts_column} < ?"));
        params.push(before_ms.to_string());
    }
    append_window_filters(
        sql,
        params,
        filter,
        &format!("{alias}.{ts_column}"),
        &format!("{alias}.id"),
        after_history_tie,
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
    // The copy is dropped only when its prompt can itself match this
    // search: a `kinds` filter without `history`, or an `origin` other than
    // `human`, excludes every prompt, so the event copy is a genuine match.
    let prompt_can_match = (filter.kinds.is_empty()
        || filter.kinds.iter().any(|kind| kind == "history"))
        && filter.origin.as_deref().is_none_or(|origin| origin == "human");
    if role != SearchRole::Assistant && prompt_can_match {
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
    append_row_filters(sql, params, filter, alias, "ts_ms", true);
    match role {
        SearchRole::All | SearchRole::Prompt => {}
        SearchRole::User => sql.push_str(&format!(" AND {alias}.role = 'user'")),
        SearchRole::Assistant => sql.push_str(&format!(" AND {alias}.role = 'assistant'")),
    }
    append_search_specific_filters(sql, params, filter, alias, false);
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
    fn echoes_containing_the_full_prompt_collapse_into_their_primary() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "please fix the login bug".into(),
                prompt_hash: Some("hash-login".into()),
                timestamp_ms: 1_000,
            },
        )
        .unwrap();
        for (uid, ts, role, kind, text) in [
            ("a1", 1_001, "assistant", "text", "Working on it: please fix the login bug"),
            ("t1", 1_002, "tool_result", "tool_result", "output: please fix the login bug"),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', 'm1', ?, ?, ?, ?, ?)",
                params![ts, role, kind, text, uid],
            )
            .unwrap();
        }
        let rows = search_all(
            &conn,
            &["login".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(rows.len(), 1, "both echoes collapse into the prompt");
        let primary = &rows[0];
        assert_eq!(primary.match_source, MATCH_SOURCE_HISTORY);
        assert_eq!(primary.group_size, 3);
        assert_eq!(primary.echo_count, 2);
        assert!(primary.is_primary);
        assert_eq!(primary.group_id.as_deref(), Some("history:1"));
        assert_eq!(primary.origin, "human");
        assert_eq!(primary.event_ref().id, primary.id);
        // The full text is never replaced by the snippet.
        assert_eq!(primary.text, "please fix the login bug");
        assert_eq!(primary.snippet, "please fix the login bug");
        assert!(!primary.content_truncated);
    }

    #[test]
    fn identical_wording_approvals_at_distinct_timestamps_both_stay_primaries() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (index, ts) in [(1, 1_000), (2, 2_000)] {
            insert_history(
                &conn,
                &HistoryEntry {
                    id: 0,
                    source: "claude".into(),
                    session_id: Some("s1".into()),
                    project: Some("/work/needle".into()),
                    prompt: "looks good ship it".into(),
                    prompt_hash: Some(format!("hash-ship-{index}")),
                    timestamp_ms: ts,
                },
            )
            .unwrap();
        }
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(rows.len(), 2, "neither approval is the other's echo");
        assert!(rows.iter().all(|row| row.group_size == 1 && row.echo_count == 0));
        assert!(rows.iter().all(|row| row.is_primary && row.group_id.is_none()));
    }

    #[test]
    fn a_snippet_centers_on_a_tail_match_and_reports_its_offset() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let text = format!("{}tailneedle", "word ".repeat(100));
        let total = text.chars().count();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: text.clone(),
                prompt_hash: Some("hash-tail".into()),
                timestamp_ms: 1_000,
            },
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["tailneedle".to_string()],
            false,
            &filter(100),
            SearchRole::Prompt,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert!(row.snippet.contains("tailneedle"), "snippet: {}", row.snippet);
        assert!(row.content_truncated);
        assert_eq!(row.snippet.chars().count(), 320);
        let offset = row.snippet_start_offset.expect("a snippet offset");
        assert!(offset > 0 && offset < total);
        let tail: String = text.chars().skip(offset).collect();
        let snippet_body: String = row.snippet.chars().skip(1).collect();
        assert_eq!(tail, snippet_body);
        assert_eq!(row.text, text, "the full text is kept beside the snippet");
    }

    #[test]
    fn a_project_only_match_falls_back_to_a_head_snippet() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let text = "word ".repeat(100);
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: text.clone(),
                prompt_hash: Some("hash-head".into()),
                timestamp_ms: 1_000,
            },
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &[],
            false,
            &QueryFilter {
                project: Some("needle".into()),
                limit: 100,
                ..Default::default()
            },
            SearchRole::Prompt,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert!(row.content_truncated);
        assert_eq!(row.snippet_start_offset, Some(0));
        assert!(row.snippet.starts_with("word word"));
        assert!(row.snippet.ends_with('\u{2026}'));
    }

    #[test]
    fn session_kinds_and_origin_filters_compose_with_role() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (session, ts) in [("s1", 1_000), ("s2", 2_000)] {
            insert_history(
                &conn,
                &HistoryEntry {
                    id: 0,
                    source: "claude".into(),
                    session_id: Some(session.into()),
                    project: Some("/work/needle".into()),
                    prompt: format!("needle prompt {session}"),
                    prompt_hash: Some(format!("hash-{session}")),
                    timestamp_ms: ts,
                },
            )
            .unwrap();
        }
        for (uid, ts, role, kind, text, control) in [
            ("e-assist", 1_100, "assistant", "text", "needle reply", None),
            ("e-tool", 1_200, "tool_result", "tool_result", "needle output", None),
            ("e-synth", 1_300, "user", "text", "needle synthetic", Some("synthetic")),
            ("e-remind", 1_400, "user", "text", "needle reminder", Some("system_reminder")),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid, control_kind) \
                 VALUES ('claude', 's1', '/work/needle', 'm1', ?, ?, ?, ?, ?, ?)",
                params![ts, role, kind, text, uid, control],
            )
            .unwrap();
        }
        let terms = ["needle".to_string()];
        let scoped = search_all(
            &conn,
            &terms,
            false,
            &QueryFilter {
                session_id: Some("s1".into()),
                limit: 100,
                ..Default::default()
            },
            SearchRole::All,
        )
        .unwrap();
        assert!(scoped.iter().all(|row| row.session_id.as_deref() == Some("s1")));
        assert!(scoped.len() < 6, "the s2 prompt is filtered out");

        let tools = search_all(
            &conn,
            &terms,
            false,
            &QueryFilter {
                kinds: vec!["tool_result".into()],
                limit: 100,
                ..Default::default()
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].kind, "tool_result");

        let synthetic = search_all(
            &conn,
            &terms,
            false,
            &QueryFilter {
                origin: Some("synthetic".into()),
                limit: 100,
                ..Default::default()
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(synthetic.len(), 1);
        assert_eq!(synthetic[0].origin, "synthetic");
        assert_eq!(synthetic[0].control_kind.as_deref(), Some("synthetic"));
        assert_eq!(synthetic[0].event_uid.as_deref(), Some("e-synth"));

        let system = search_all(
            &conn,
            &terms,
            false,
            &QueryFilter {
                origin: Some("system".into()),
                limit: 100,
                ..Default::default()
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(system.len(), 1);
        assert_eq!(system[0].origin, "system");

        let human = search_all(
            &conn,
            &terms,
            false,
            &QueryFilter {
                origin: Some("human".into()),
                limit: 100,
                ..Default::default()
            },
            SearchRole::All,
        )
        .unwrap();
        assert!(human.iter().all(|row| row.origin == "human"));
        assert!(human.iter().any(|row| row.match_source == MATCH_SOURCE_HISTORY));

        let bad_kind = search_all(
            &conn,
            &terms,
            false,
            &QueryFilter {
                kinds: vec!["bogus".into()],
                limit: 100,
                ..Default::default()
            },
            SearchRole::All,
        )
        .unwrap_err()
        .to_string();
        assert!(bad_kind.contains("search kind must be one of"), "got: {bad_kind}");
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

    #[test]
    fn deep_pages_inside_a_large_tie_match_the_sorted_reference() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // Ten events share the newest timestamp: more than any test window,
        // so a page that starts at the tie falls back, while a deep page
        // whose cursor has passed most of it walks the rest.
        let insert = |index: i64, ts: i64| {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work', 'm', ?, 'assistant', 'text', ?, ?)",
                params![ts, format!("needle {index}"), format!("e{index}")],
            )
            .unwrap();
        };
        for index in 0..10 {
            insert(index, 5_000);
        }
        for index in 10..20 {
            insert(index, 1_000 + index);
        }
        WALK_OUTCOMES.with(|outcomes| outcomes.set((0, 0)));
        let terms = ["needle".to_string()];
        let mut after = None;
        let mut pages = 0;
        loop {
            let filter = QueryFilter {
                limit: 1,
                after: after.clone(),
                ..Default::default()
            };
            let expected = sorted_reference(|| {
                search_page(&conn, &terms, false, &filter, SearchRole::Assistant).unwrap()
            });
            let actual = search_page(&conn, &terms, false, &filter, SearchRole::Assistant).unwrap();
            assert_eq!(keys(&actual.rows), keys(&expected.rows), "page {pages}");
            assert_eq!(actual.next_cursor, expected.next_cursor, "page {pages}");
            pages += 1;
            match actual.next_cursor {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        assert_eq!(pages, 20);
        let (served, fell_back) = WALK_OUTCOMES.with(|outcomes| outcomes.get());
        assert!(served > 0 && fell_back > 0, "served {served}, fell back {fell_back}");
    }

    #[test]
    fn the_latest_prompt_with_a_session_skips_newer_session_less_prompts() {
        let conn = walk_fixture();
        // Newer prompts than any fixture row, none naming a session.
        for index in 0..10 {
            insert_history(
                &conn,
                &HistoryEntry {
                    id: 0,
                    source: "claude".into(),
                    session_id: if index % 2 == 0 { None } else { Some(String::new()) },
                    project: Some("/work/alpha".into()),
                    prompt: format!("needle orphan {index}"),
                    prompt_hash: None,
                    timestamp_ms: 9_000 + index,
                },
            )
            .unwrap();
        }
        let terms = ["needle".to_string()];
        for walk in [true, false] {
            let read = || {
                latest_prompt_with_session(&conn, &terms, false, &QueryFilter::default()).unwrap()
            };
            let found = if walk { read() } else { sorted_reference(read) };
            let expected = search_all(&conn, &terms, false, &filter(1_000), SearchRole::Prompt)
                .unwrap()
                .into_iter()
                .find(|row| row.session_id.as_deref().is_some_and(|id| !id.is_empty()))
                .unwrap();
            let found = found.expect("a resumable prompt");
            assert_eq!((found.id, found.timestamp_ms), (expected.id, expected.timestamp_ms));
        }
    }
}
