//! Typed history and event search, independent of command-line formatting.
//!
//! This is the one search contract every surface uses: the CLI's `search`,
//! the napi `search` the TypeScript SDK wraps, and the MCP `search_history`
//! tool. It matches user prompts (`history`) and normalized session events
//! (`session_events`) through their FTS5 indexes, applies the same
//! scope/source/project/tag/time filters to both, and orders the merged result
//! by `(timestamp_ms DESC, id DESC, match_source)` so a tie never reorders
//! between calls.
use crate::{raw_fts_query_error, HistoryCursor, HistoryPage, QueryFilter};
#[cfg(test)]
use crate::SessionScope;
use anyhow::Result;
use rusqlite::Connection;
#[cfg(test)]
use walk::{SORT_EVERY_MATCH, WALK_OUTCOMES};
use filters::{append_event_search_filters, append_history_search_filters};
use walk::{walk, walk_pays, BranchSearch};

pub use presentation::{compute_snippet, DEFAULT_SNIPPET_CHARS};

mod filters;
mod grouping;
mod page;
mod presentation;
mod walk;

use page::{fetch_grouped_window, present_rows, resolve_echoes};

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

/// Reference to the stored row a match came from. For a `session_event`
/// match it names the event `get_session_events` can re-read (by session,
/// around the event's position). For a `history` match, `id` identifies a
/// history row, not a session event — re-read it through the prompt/search
/// surfaces, and note a history match may carry no session id at all.
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



/// The per-branch row bound for the echo recount's un-cursored universe
/// fetch. It is deliberately larger than [`GROUP_FETCH_GROWTH_CAP`]: the
/// page fetch itself grows only to that cap, and this bound applies to the
/// recount's session-scoped refetch. When it stops a recount, the affected
/// primaries keep `group_truncated` instead of silently claiming complete
/// counts.
const MAX_GROUP_FETCH: i64 = 50_000;

/// The per-branch bound the stability growth in [`search_all`] may reach.
/// Past it the page is accepted as-is rather than read exhaustively: a
/// standalone assistant/tool match can only be absorbed by a primary that
/// also matches this search, and proving none exists anywhere in a large
/// match set would make a one-row search cost the whole corpus. Rows that
/// might still be such an echo are flagged `group_truncated`, and
/// [`search_page`] still returns a cursor, so the bound is visible to the
/// caller instead of silently wrong.
const GROUP_FETCH_GROWTH_CAP: i64 = 2_000;

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
    /// Internal: the page fetch stopped at its growth cap before the page
    /// was provably complete, so older matches may follow even when this
    /// page came back short. Kept separate from `group_truncated` (which
    /// describes one group's echo count) because the echo recount may
    /// complete a group — clearing its flag — without completing the page.
    /// Not part of the wire contract; the napi layer does not map it.
    pub page_incomplete: bool,
    /// Internal: further transcript ids this row is known by (every
    /// mirrored event's ids for a history prompt). Echo linkage checks
    /// them alongside `message_id`/`event_uid`/`tool_use_id`. Not part of
    /// the wire contract; the napi layer does not map it.
    pub alt_ids: Vec<String>,
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
    // Grouping happens before the limit: echoes collapse into their primary
    // and only primaries count toward the page, in the primary's position in
    // the global (timestamp_ms DESC, id DESC, match_source ASC) order, so
    // cursors taken from a page stay valid. A single bounded fetch can end
    // the page early — echoes collapsing leave fewer primaries than the
    // limit while older matches wait beyond the bound, and an echo whose
    // primary lies beyond it would stand alone — so the fetch bound grows
    // (doubling, up to GROUP_FETCH_GROWTH_CAP rows per branch) and the rows
    // are regrouped until the page is provably complete, a branch is truly
    // exhausted, or the growth cap accepts the page with its possibly-echo
    // rows flagged `group_truncated`.
    // The window advances only when echo resolution empties a page the
    // growth cap left incomplete: the dropped echoes were already counted,
    // and older matches beyond them must stay reachable.
    let mut window = filter.clone();
    let mut continuations = 0;
    let mut grouped: Vec<SearchRow>;
    'pages: loop {
        let (rows, page_stable) =
            fetch_grouped_window(conn, terms, raw_fts, &window, role, requested)?;
        if window.after.is_none() {
            grouped = rows;
            break;
        }
        // A page cursor must not change a primary's echo count, and an echo
        // older than its primary must not resurface as its own match:
        // resolve counts and membership against the un-cursored match set
        // BEFORE the limit selects the page, so a dropped echo never
        // consumes a page slot.
        let (rows, continuation) =
            resolve_echoes(conn, terms, raw_fts, &window, role, rows, page_stable)?;
        grouped = rows;
        match continuation {
            Some(cursor) if continuations < 64 => {
                continuations += 1;
                window.after = Some(cursor);
                continue 'pages;
            }
            _ => break,
        }
    }
    grouped.truncate(requested as usize);
    present_rows(&mut grouped, terms, filter);
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
    } else if rows.iter().any(|row| row.group_truncated || row.page_incomplete) {
        // The grouping fetch hit its bound, so this page may not be the
        // last one even though it came back short: hand back a cursor
        // rather than silently claiming exhaustion. `page_incomplete` is
        // the reliable signal here — the echo recount can complete a
        // group's count and clear its `group_truncated`, but it cannot
        // complete the page.
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
    // A history row carries no transcript ids of its own, but its prompt is
    // usually mirrored by a user text event (the same provider record).
    // That mirror is suppressed from the event branch as a duplicate, so
    // borrow its message/event ids here: an echo linked to the mirror's id
    // can then be attributed to this prompt instead of whichever identical
    // prompt the text alone would pick. Every matching mirror contributes
    // its ids (a JSON array), not just the first, in case a prompt was
    // recorded by more than one event. The trim set inlined below mirrors
    // `filters::RUST_WHITESPACE` (concat! takes literals only).
    columns: concat!(
        "h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms, 'user', 'history', ",
        "(SELECT json_group_array(e.message_id) FROM session_events e WHERE e.source = h.source ",
        "AND e.session_id = h.session_id AND e.ts_ms = h.timestamp_ms ",
        "AND e.role = 'user' AND e.kind = 'text' AND trim(e.text, ",
        "char(9, 10, 11, 12, 13, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, ",
        "8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288)",
        ") = h.prompt), NULL, ",
        "(SELECT json_group_array(e.event_uid) FROM session_events e WHERE e.source = h.source ",
        "AND e.session_id = h.session_id AND e.ts_ms = h.timestamp_ms ",
        "AND e.role = 'user' AND e.kind = 'text' AND trim(e.text, ",
        "char(9, 10, 11, 12, 13, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, ",
        "8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288)",
        ") = h.prompt), NULL, NULL, NULL"
    ),
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
    // The history branch collects every mirrored event's ids as a JSON
    // array (lossless for ids containing commas or separators); the first
    // of each list stays the row's own id field and the rest are
    // alternates echo linkage may name.
    let mut message_id: Option<String> = row.get(8)?;
    let mut event_uid: Option<String> = row.get(10)?;
    let mut alt_ids: Vec<String> = Vec::new();
    if match_source == MATCH_SOURCE_HISTORY {
        let split = |value: Option<String>, alt_ids: &mut Vec<String>| {
            let mut parts: Vec<String> = value
                .and_then(|v| serde_json::from_str::<Vec<Option<String>>>(&v).ok())
                .unwrap_or_default()
                .into_iter()
                .flatten()
                .collect();
            if parts.is_empty() {
                None
            } else {
                let first = parts.remove(0);
                alt_ids.append(&mut parts);
                Some(first)
            }
        };
        message_id = split(message_id, &mut alt_ids);
        event_uid = split(event_uid, &mut alt_ids);
    }
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
        message_id,
        parent_id: row.get(9)?,
        event_uid,
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
        page_incomplete: false,
        alt_ids,
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
    fn origin_unknown_returns_ordinary_assistant_rows_and_filters_agree_with_labels() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "needle prompt".into(),
                prompt_hash: Some("hash-origin".into()),
                timestamp_ms: 1_000,
            },
        )
        .unwrap();
        for (uid, ts, role, text, control) in [
            ("e-assist", 1_100, "assistant", "needle ordinary reply", None),
            ("e-custom", 1_200, "assistant", "needle custom reply", Some("custom")),
            ("e-synth", 1_300, "user", "needle synthetic note", Some("synthetic")),
            ("e-remind", 1_400, "user", "needle reminder note", Some("system_reminder")),
            ("e-human", 1_500, "user", "needle human note", None),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid, control_kind) \
                 VALUES ('claude', 's1', '/work/needle', 'm1', ?, ?, 'text', ?, ?, ?)",
                params![ts, role, text, uid, control],
            )
            .unwrap();
        }
        let terms = ["needle".to_string()];
        let unknown = search_all(
            &conn,
            &terms,
            false,
            &QueryFilter {
                origin: Some("unknown".into()),
                limit: 100,
                ..Default::default()
            },
            SearchRole::All,
        )
        .unwrap();
        let unknown_uids: Vec<&str> = unknown
            .iter()
            .filter_map(|row| row.event_uid.as_deref())
            .collect();
        assert_eq!(unknown_uids, vec!["e-custom", "e-assist"]);
        assert!(unknown.iter().all(|row| row.origin == "unknown"));
        // Every origin filter returns only rows whose reported origin agrees.
        for origin in ["human", "synthetic", "system", "unknown"] {
            let rows = search_all(
                &conn,
                &terms,
                false,
                &QueryFilter {
                    origin: Some(origin.into()),
                    limit: 100,
                    ..Default::default()
                },
                SearchRole::All,
            )
            .unwrap();
            assert!(
                rows.iter().all(|row| row.origin == origin),
                "origin {origin} returned {rows:?}"
            );
        }
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
        assert!(human
            .iter()
            .any(|row| row.match_source == MATCH_SOURCE_HISTORY));
    }

    #[test]
    fn grouped_pages_reach_older_matches_beyond_the_echo_fetch_bound() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-new', 5000, 'user', 'text', 'needle grouped prompt', 'p-new')",
            [],
        )
        .unwrap();
        for index in 0..9 {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', 'm-echo', ?, 'assistant', 'text', 'echo: needle grouped prompt', ?)",
                params![5_001 + index, format!("e{index}")],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's2', '/work/needle', 'm-old', 1000, 'user', 'text', 'needle older prompt', 'p-old')",
            [],
        )
        .unwrap();
        let terms = ["needle".to_string()];
        let kinds = QueryFilter {
            kinds: vec!["text".into()],
            limit: 1,
            ..Default::default()
        };
        let first = search_page(&conn, &terms, false, &kinds, SearchRole::All).unwrap();
        assert_eq!(first.rows.len(), 1);
        assert_eq!(first.rows[0].text, "needle grouped prompt");
        assert_eq!(first.rows[0].echo_count, 9);
        assert_eq!(first.rows[0].group_size, 10);
        let cursor = first.next_cursor.expect("an older match follows");
        let second = search_page(
            &conn,
            &terms,
            false,
            &QueryFilter {
                after: Some(cursor),
                ..kinds
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0].text, "needle older prompt");
        assert!(second.next_cursor.is_none());
    }

    #[test]
    fn an_echo_newer_than_its_primary_within_the_bound_still_groups() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "needle bounded prompt".into(),
                prompt_hash: Some("hash-bounded".into()),
                timestamp_ms: 1_000,
            },
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm1', 2000, 'assistant', 'text', 'reply: needle bounded prompt', 'e1')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["needle".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].echo_count, 1);
    }

    #[test]
    fn echo_counts_do_not_depend_on_the_page_cursor() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (index, ts, prompt) in [
            (1, 1_000, "needle alpha prompt"),
            (2, 2_000, "needle beta prompt"),
        ] {
            insert_history(
                &conn,
                &HistoryEntry {
                    id: 0,
                    source: "claude".into(),
                    session_id: Some("s1".into()),
                    project: Some("/work/needle".into()),
                    prompt: prompt.into(),
                    prompt_hash: Some(format!("hash-echo-{index}")),
                    timestamp_ms: ts,
                },
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm1', 3000, 'assistant', 'text', 'quoting needle alpha prompt', 'e-alpha')",
            [],
        )
        .unwrap();
        let terms = ["needle".to_string()];
        let one_page = search_all(&conn, &terms, false, &filter(100), SearchRole::All).unwrap();
        let alpha_count = one_page
            .iter()
            .find(|row| row.text == "needle alpha prompt")
            .map(|row| row.echo_count);
        assert_eq!(alpha_count, Some(1));

        let first = search_page(&conn, &terms, false, &filter(1), SearchRole::All).unwrap();
        assert_eq!(first.rows.len(), 1);
        assert_eq!(first.rows[0].text, "needle beta prompt");
        let cursor = first.next_cursor.expect("the alpha prompt follows");
        let second = search_page(
            &conn,
            &terms,
            false,
            &QueryFilter {
                after: Some(cursor),
                ..filter(1)
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0].text, "needle alpha prompt");
        assert_eq!(second.rows[0].echo_count, 1, "same count as the one-page read");
        assert_eq!(second.rows[0].group_size, 2);
    }

    #[test]
    fn assistant_only_matches_page_without_a_primary() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for index in 0..30 {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', ?, ?, 'assistant', 'text', ?, ?)",
                params![
                    format!("m{index}"),
                    1_000 + index,
                    format!("needle assistant reply {index}"),
                    format!("e{index}")
                ],
            )
            .unwrap();
        }
        let terms = ["needle".to_string()];
        let first = search_page(&conn, &terms, false, &filter(5), SearchRole::All).unwrap();
        assert_eq!(first.rows.len(), 5);
        assert!(first.rows.iter().all(|row| row.role == "assistant"));
        let cursor = first.next_cursor.expect("more assistant replies follow");
        let second = search_page(
            &conn,
            &terms,
            false,
            &QueryFilter {
                after: Some(cursor),
                ..filter(5)
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(second.rows.len(), 5);
        let first_ids: Vec<i64> = first.rows.iter().map(|row| row.id).collect();
        assert!(second.rows.iter().all(|row| !first_ids.contains(&row.id)));
    }

    #[test]
    fn assistant_only_matches_stop_at_the_growth_cap_and_flag_the_page() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut batch = String::new();
        for index in 0..2_100 {
            batch.push_str(&format!(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', 'm{index}', {}, 'assistant', 'text', \
                 'needle assistant reply {index}', 'e{index}');",
                1_000 + index
            ));
        }
        conn.execute_batch(&batch).unwrap();
        let rows = search_all(
            &conn,
            &["needle".to_string()],
            false,
            &filter(1),
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0].group_truncated,
            "a page accepted at the growth cap flags its possibly-echo rows"
        );
    }

    #[test]
    fn continued_pages_reach_matches_beyond_the_grouping_cap() {
        // Devin's scenario: a newer primary A, then 2,000 assistant echoes
        // of an older primary B filling the whole event fetch bound, then a
        // still older primary C in the same branch. Page two's recount
        // completes B's echo count; that must not cost C its cursor.
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (session, ts, prompt) in [
            ("sa", 10_000, "needle alpha prompt"),
            ("sb", 1_500, "needle bravo prompt"),
        ] {
            insert_history(
                &conn,
                &HistoryEntry {
                    id: 0,
                    source: "claude".into(),
                    session_id: Some(session.into()),
                    project: Some("/work/needle".into()),
                    prompt: prompt.into(),
                    prompt_hash: Some(format!("hash-{session}")),
                    timestamp_ms: ts,
                },
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 'sc', '/work/needle', 'mc', 1000, 'user', 'text', 'needle charlie prompt', 'ec')",
            [],
        )
        .unwrap();
        let mut batch = String::new();
        for index in 0..2_000 {
            batch.push_str(&format!(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 'sb', '/work/needle', 'm{index}', {}, 'assistant', 'text', \
                 'quoting needle bravo prompt {index}', 'e{index}');",
                2_000 + index
            ));
        }
        conn.execute_batch(&batch).unwrap();

        let terms = ["needle".to_string()];
        let first = search_page(&conn, &terms, false, &filter(1), SearchRole::All).unwrap();
        assert_eq!(first.rows.len(), 1);
        assert_eq!(first.rows[0].text, "needle alpha prompt");
        let cursor = first.next_cursor.expect("bravo follows alpha");

        let second = search_page(
            &conn,
            &terms,
            false,
            &QueryFilter {
                after: Some(cursor),
                ..filter(1)
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0].text, "needle bravo prompt");
        assert_eq!(second.rows[0].echo_count, 2_000);
        let cursor = second
            .next_cursor
            .expect("charlie stays reachable past the grouping cap");

        let third = search_page(
            &conn,
            &terms,
            false,
            &QueryFilter {
                after: Some(cursor),
                ..filter(1)
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(third.rows.len(), 1);
        assert_eq!(third.rows[0].text, "needle charlie prompt");
        assert!(third.next_cursor.is_none());
    }

    #[test]
    fn a_text_echo_counts_under_the_closest_preceding_identical_prompt() {
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
                    prompt: "ship it".into(),
                    prompt_hash: Some(format!("hash-precedence-{index}")),
                    timestamp_ms: ts,
                },
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm1', 1001, 'assistant', 'text', 'assistant: ship it', 'e-quote')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        let older = rows.iter().find(|row| row.timestamp_ms == 1_000).unwrap();
        let newer = rows.iter().find(|row| row.timestamp_ms == 2_000).unwrap();
        assert_eq!(older.echo_count, 1);
        assert_eq!(newer.echo_count, 0);
    }

    #[test]
    fn an_id_linked_echo_counts_under_its_linked_prompt_over_a_text_match() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (uid, message, ts) in [("p-old", "m-old", 1_000), ("p-new", "m-new", 2_000)] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', ?, ?, 'user', 'text', 'ship it', ?)",
                params![message, ts, uid],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, parent_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-echo', 'm-old', 2001, 'assistant', 'text', 'assistant: ship it', 'e-link')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        let older = rows.iter().find(|row| row.timestamp_ms == 1_000).unwrap();
        let newer = rows.iter().find(|row| row.timestamp_ms == 2_000).unwrap();
        assert_eq!(older.echo_count, 1);
        assert_eq!(newer.echo_count, 0);
    }

    #[test]
    fn a_parented_reply_that_does_not_repeat_the_prompt_stays_a_match() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-user', 1000, 'user', 'text', 'ship it', 'e-user')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, parent_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-reply', 'm-user', 1001, 'assistant', 'text', 'we will ship tomorrow after tests', 'e-reply')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(rows.len(), 2, "ancestry alone must not suppress the reply");
        let prompt = rows.iter().find(|row| row.text == "ship it").unwrap();
        assert_eq!(prompt.echo_count, 0);
    }

    #[test]
    fn a_provider_id_colliding_with_a_row_id_does_not_link() {
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
                    prompt: "ship it".into(),
                    prompt_hash: Some(format!("hash-collide-{index}")),
                    timestamp_ms: ts,
                },
            )
            .unwrap();
        }
        // The first inserted history row has SQLite id 1; a provider
        // parent_id of "1" names no transcript id of either prompt, so the
        // echo (which repeats the prompt) joins by text: the closest
        // preceding primary, at ts 2000.
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, parent_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-echo', '1', 2001, 'assistant', 'text', 'assistant: ship it', 'e-collide')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        let older = rows.iter().find(|row| row.timestamp_ms == 1_000).unwrap();
        let newer = rows.iter().find(|row| row.timestamp_ms == 2_000).unwrap();
        assert_eq!(older.echo_count, 0);
        assert_eq!(newer.echo_count, 1);
    }

    #[test]
    fn an_echo_older_than_its_primary_is_not_returned_again_after_the_cursor() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "needle prompt".into(),
                prompt_hash: Some("hash-older-echo".into()),
                timestamp_ms: 2_000,
            },
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-echo', 1000, 'assistant', 'text', 'earlier copy: needle prompt', 'e-old')",
            [],
        )
        .unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "needle tail prompt".into(),
                prompt_hash: Some("hash-tail".into()),
                timestamp_ms: 500,
            },
        )
        .unwrap();
        let terms = ["needle".to_string()];
        let first = search_page(&conn, &terms, false, &filter(1), SearchRole::All).unwrap();
        assert_eq!(first.rows[0].text, "needle prompt");
        assert_eq!(first.rows[0].echo_count, 1);
        let second = search_page(
            &conn,
            &terms,
            false,
            &QueryFilter {
                after: first.next_cursor,
                ..filter(1)
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0].text, "needle tail prompt");
    }

    #[test]
    fn echoes_do_not_consume_a_continued_page() {
        // The prompt's echoes sort between it and the next primary. They
        // were already counted on the prompt's page, so the continued page
        // must reach the next primary instead of returning (or stalling
        // on) the echoes again.
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "needle prompt".into(),
                prompt_hash: Some("hash-echoes-alone".into()),
                timestamp_ms: 2_000,
            },
        )
        .unwrap();
        for (index, ts) in [(1, 1_000), (2, 1_001)] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', ?, ?, 'assistant', 'text', ?, ?)",
                params![
                    format!("m-echo-{index}"),
                    ts,
                    format!("needle prompt (copy {index})"),
                    format!("e-echo-{index}")
                ],
            )
            .unwrap();
        }
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "needle tail prompt".into(),
                prompt_hash: Some("hash-echoes-tail".into()),
                timestamp_ms: 500,
            },
        )
        .unwrap();
        let terms = ["needle".to_string()];
        let first = search_page(&conn, &terms, false, &filter(1), SearchRole::All).unwrap();
        assert_eq!(first.rows[0].text, "needle prompt");
        assert_eq!(first.rows[0].echo_count, 2);
        let second = search_page(
            &conn,
            &terms,
            false,
            &QueryFilter {
                after: first.next_cursor,
                ..filter(1)
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0].text, "needle tail prompt");
        assert!(second.next_cursor.is_none());
    }

    #[test]
    fn an_echo_only_capped_page_continues_to_older_matches() {
        // Devin's scenario: a prompt, then more echoes than the grouping
        // fetch cap, then an older prompt. The capped continued page holds
        // only echoes; resolving them must not strand the older prompt.
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (session, ts, prompt) in [
            ("sa", 5_000, "needle cap prompt"),
            ("sb", 100, "needle older prompt"),
        ] {
            insert_history(
                &conn,
                &HistoryEntry {
                    id: 0,
                    source: "claude".into(),
                    session_id: Some(session.into()),
                    project: Some("/work/needle".into()),
                    prompt: prompt.into(),
                    prompt_hash: Some(format!("hash-cap-{session}")),
                    timestamp_ms: ts,
                },
            )
            .unwrap();
        }
        let mut batch = String::new();
        for index in 0..2_001 {
            batch.push_str(&format!(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 'sa', '/work/needle', 'm{index}', {}, 'assistant', 'text', \
                 'needle cap prompt (echo {index})', 'e{index}');",
                1_000 + index
            ));
        }
        conn.execute_batch(&batch).unwrap();
        let terms = ["needle".to_string()];
        let first = search_page(&conn, &terms, false, &filter(1), SearchRole::All).unwrap();
        assert_eq!(first.rows[0].text, "needle cap prompt");
        let second = search_page(
            &conn,
            &terms,
            false,
            &QueryFilter {
                after: first.next_cursor,
                ..filter(1)
            },
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0].text, "needle older prompt");
    }

    #[test]
    fn a_snippet_does_not_center_on_a_sharp_s_fold_the_search_did_not_match() {
        let mut text = "Strasse ".to_string();
        text.push_str(&"filler ".repeat(80));
        text.push_str("Straße");
        let (snippet, _, offset) = compute_snippet(&text, &["strasse".to_string()], 40);
        assert!(snippet.contains("Strasse"), "snippet: {snippet}");
        assert_eq!(offset, Some(0));
        let (snippet, _, _) = compute_snippet(&text, &["straße".to_string()], 40);
        assert!(snippet.contains("Straße"), "snippet: {snippet}");
    }

    #[test]
    fn an_echo_linked_to_a_mirrored_prompt_uses_that_prompts_id() {
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
                    prompt: "ship it".into(),
                    prompt_hash: Some(format!("hash-mirror-{index}")),
                    timestamp_ms: ts,
                },
            )
            .unwrap();
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', ?, ?, 'user', 'text', 'ship it', ?)",
                params![format!("u{index}"), ts, format!("eu{index}")],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, parent_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-echo', 'u1', 2001, 'assistant', 'text', 'assistant: ship it', 'e-mirror-echo')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        let older = rows.iter().find(|row| row.timestamp_ms == 1_000).unwrap();
        let newer = rows.iter().find(|row| row.timestamp_ms == 2_000).unwrap();
        assert_eq!(older.echo_count, 1, "the linked mirror id attributes the echo");
        assert_eq!(newer.echo_count, 0);
    }

    #[test]
    fn a_reply_quoting_the_prompt_mid_sentence_stays_a_match() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-user', 1000, 'user', 'text', 'ship it', 'e-user')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, parent_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-reply', 'm-user', 1001, 'assistant', 'text', 'Do not ship it: the tests failed', 'e-reply')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(rows.len(), 2, "a mid-sentence quote is a substantive match");
    }

    #[test]
    fn a_snippet_centers_on_a_column_filter_operand_and_folds_diacritics() {
        let (snippet, _, _) = compute_snippet(
            "café opening times and the tailneedle location",
            &["text:tailneedle".to_string()],
            20,
        );
        assert!(snippet.contains("tailneedle"), "snippet: {snippet}");
        let (snippet, _, _) = compute_snippet(
            "meet at the café at noon, bring the needle thread",
            &["cafe".to_string()],
            20,
        );
        assert!(snippet.contains("café"), "snippet: {snippet}");
    }

    #[test]
    fn a_reply_starting_with_the_prompt_but_adding_substance_stays_a_match() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-user', 1000, 'user', 'text', 'ship it', 'e-user')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, parent_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-reply', 'm-user', 1001, 'assistant', 'text', 'ship it only after fixing these failures in the importer and rerunning the whole suite', 'e-reply')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        assert_eq!(rows.len(), 2, "a prefix alone must not make a long reply an echo");
    }

    #[test]
    fn an_echo_linked_to_any_mirrored_event_id_uses_the_history_prompt() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "ship it".into(),
                prompt_hash: Some("hash-two-mirrors".into()),
                timestamp_ms: 1_000,
            },
        )
        .unwrap();
        for (index, ts) in [(1, 1_000), (2, 1_000)] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', ?, ?, 'user', 'text', 'ship it', ?)",
                params![format!("mirror{index}"), ts, format!("em{index}")],
            )
            .unwrap();
        }
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "ship it".into(),
                prompt_hash: Some("hash-two-mirrors-new".into()),
                timestamp_ms: 2_000,
            },
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, parent_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-echo', 'mirror2', 2001, 'assistant', 'text', 'assistant: ship it', 'e-echo')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        let older = rows.iter().find(|row| row.timestamp_ms == 1_000).unwrap();
        let newer = rows.iter().find(|row| row.timestamp_ms == 2_000).unwrap();
        assert_eq!(older.echo_count, 1);
        assert_eq!(newer.echo_count, 0);
    }

    #[test]
    fn a_snippet_centers_on_the_standalone_token_not_an_earlier_substring() {
        let mut text = "needlepoint ".to_string();
        text.push_str(&"filler ".repeat(80));
        text.push_str("needle");
        let (snippet, _, offset) = compute_snippet(&text, &["needle".to_string()], 40);
        assert!(snippet.contains(" needle") || snippet.ends_with("needle"), "snippet: {snippet}");
        assert!(offset.unwrap() > 100, "offset past the substring hit: {offset:?}");
        let (snippet, _, _) = compute_snippet("the Škoda needle thread", &["skoda".to_string()], 40);
        assert!(snippet.contains("Škoda"), "snippet: {snippet}");
    }

    #[test]
    fn a_snippet_skips_an_embedded_unicode_occurrence_without_panicking() {
        let mut text = "котик ".to_string();
        text.push_str(&"filler ".repeat(80));
        text.push_str("кот");
        let (snippet, _, offset) = compute_snippet(&text, &["кот".to_string()], 40);
        assert!(snippet.ends_with("кот") || snippet.contains(" кот"), "snippet: {snippet}");
        assert!(offset.unwrap() > 100, "offset past the embedded hit: {offset:?}");
    }

    #[test]
    fn a_mirror_id_containing_a_comma_still_links() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "ship it".into(),
                prompt_hash: Some("hash-comma-mirror".into()),
                timestamp_ms: 1_000,
            },
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'id,with,commas', 1000, 'user', 'text', 'ship it', 'e-comma')",
            [],
        )
        .unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "ship it".into(),
                prompt_hash: Some("hash-comma-new".into()),
                timestamp_ms: 2_000,
            },
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, parent_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-echo', 'id,with,commas', 2001, 'assistant', 'text', 'assistant: ship it', 'e-echo')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        let older = rows.iter().find(|row| row.timestamp_ms == 1_000).unwrap();
        let newer = rows.iter().find(|row| row.timestamp_ms == 2_000).unwrap();
        assert_eq!(older.echo_count, 1);
        assert_eq!(newer.echo_count, 0);
    }

    #[test]
    fn a_snippet_with_a_one_codepoint_window_at_the_match_end_keeps_the_match() {
        let (snippet, truncated, offset) = compute_snippet("abc", &["c".to_string()], 1);
        assert!(truncated);
        assert_eq!(snippet, "c");
        assert_eq!(offset, Some(2));
    }

    #[test]
    fn a_snippet_with_a_one_codepoint_window_centers_without_panicking() {
        let (snippet, truncated, offset) = compute_snippet("abc", &["b".to_string()], 1);
        assert!(truncated);
        assert_eq!(snippet, "b");
        assert_eq!(offset, Some(1));
    }

    #[test]
    fn a_snippet_offset_counts_original_codepoints_past_expanding_lowercase() {
        // `İ` lowercases to two codepoints, so an offset counted in the
        // lowered text would run 60 codepoints past the original position.
        let text = format!("{}needle{}", "\u{130}".repeat(60), "y".repeat(100));
        let (snippet, truncated, offset) = compute_snippet(&text, &["needle".to_string()], 40);
        assert!(truncated);
        assert!(snippet.contains("needle"), "snippet: {snippet}");
        let offset = offset.expect("a snippet offset");
        assert_eq!(offset, 45);
        let chars: Vec<char> = text.chars().collect();
        let body: Vec<char> = snippet.chars().skip(1).take(snippet.chars().count() - 2).collect();
        assert_eq!(chars[offset..offset + body.len()], body[..]);
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

    #[test]
    fn a_snippet_prefers_a_whole_token_from_any_term_over_an_earlier_substring() {
        let text = "alphabet soup then beta here and more words follow on";
        let (snippet, _, _) = compute_snippet(text, &["alpha OR beta".to_string()], 16);
        assert!(snippet.contains("beta"), "snippet: {snippet}");
        assert!(!snippet.contains("alphabet"), "snippet: {snippet}");
    }

    #[test]
    fn a_snippet_centers_a_prefix_term_at_a_token_start() {
        let text = "concatenate and catapult away now";
        let (snippet, _, _) = compute_snippet(text, &["cat*".to_string()], 14);
        assert!(snippet.contains("catapult"), "snippet: {snippet}");
        assert!(!snippet.contains("concatenate"), "snippet: {snippet}");
    }

    #[test]
    fn a_snippet_centers_on_a_quoted_column_operand() {
        let text = format!("{} tailneedle done", "filler ".repeat(30));
        let (snippet, _, _) =
            compute_snippet(&text, &["text:\"tailneedle\"".to_string()], 40);
        assert!(snippet.contains("tailneedle"), "snippet: {snippet}");
    }

    #[test]
    fn a_snippet_folds_latin_extended_additional_diacritics() {
        let text = "a ḍelta value follows here";
        let (snippet, _, _) = compute_snippet(text, &["delta".to_string()], 12);
        assert!(snippet.contains("ḍelta"), "snippet: {snippet}");
    }

    #[test]
    fn an_escaped_quote_tool_payload_still_collapses_as_an_echo() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (uid, role, kind, text, ts) in [
            ("e-user", "user", "text", "run \"ship it\" now", 1_000),
            (
                "e-tool",
                "assistant",
                "tool_use",
                "{\"cmd\": \"run \\\"ship it\\\" now\"}",
                1_001,
            ),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', ?1, ?2, ?3, ?4, ?5, ?1)",
                rusqlite::params![uid, ts, role, kind, text],
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
        assert_eq!(rows.len(), 1, "rows: {rows:?}");
        assert_eq!(rows[0].echo_count, 1);
    }

    #[test]
    fn a_short_negating_reply_is_not_an_echo() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (uid, role, text, ts) in [
            ("e-user", "user", "ship it", 1_000),
            ("e-asst", "assistant", "Do not ship it", 1_001),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', ?1, ?2, ?3, 'text', ?4, ?1)",
                rusqlite::params![uid, ts, role, text],
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
        assert_eq!(rows.len(), 2, "rows: {rows:?}");
    }

    #[test]
    fn a_short_rejection_after_the_prompt_is_not_an_echo() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (uid, role, text, ts) in [
            ("e-user", "user", "ship it", 1_000),
            ("e-asst", "assistant", "ship it? No.", 1_001),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', ?1, ?2, ?3, 'text', ?4, ?1)",
                rusqlite::params![uid, ts, role, text],
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
        assert_eq!(rows.len(), 2, "rows: {rows:?}");
    }

    #[test]
    fn a_prompt_containing_escaped_quotes_still_collapses_its_verbatim_echo() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (uid, role, kind, text, ts) in [
            ("e-user", "user", "text", "run \\\"ship it\\\" now", 1_000),
            (
                "e-tool",
                "assistant",
                "tool_use",
                "{\"cmd\": \"run \\\"ship it\\\" now\"}",
                1_001,
            ),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', '/work/needle', ?1, ?2, ?3, ?4, ?5, ?1)",
                rusqlite::params![uid, ts, role, kind, text],
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
        assert_eq!(rows.len(), 1, "rows: {rows:?}");
        assert_eq!(rows[0].echo_count, 1);
    }

    #[test]
    fn a_snippet_centers_on_a_quoted_prefix_column_operand() {
        let text = format!("concatenate {} catapult done", "filler ".repeat(20));
        let (snippet, _, _) = compute_snippet(&text, &["text:\"cat\"*".to_string()], 30);
        assert!(snippet.contains("catapult"), "snippet: {snippet}");
    }

    #[test]
    fn a_snippet_folds_dotted_s_and_keeps_multi_diacritic_letters_distinct() {
        let text = format!("{} ṡample end", "filler ".repeat(30));
        let (snippet, _, _) = compute_snippet(&text, &["sample".to_string()], 30);
        assert!(snippet.contains("ṡample"), "snippet: {snippet}");
        // ḹ carries two diacritics; the tokenizer keeps it distinct from
        // `l`, so a search for `l` centers on the standalone tail `l`.
        let text = format!("ḹ{} tail l here", "x".repeat(60));
        let (snippet, _, _) = compute_snippet(&text, &["l".to_string()], 20);
        assert!(snippet.contains("tail l"), "snippet: {snippet}");
    }

    #[test]
    fn session_less_rows_never_group_with_each_other() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for (uid, role, text, ts) in [
            ("e-user", "user", "ship it", 1_000),
            ("e-asst", "assistant", "assistant: ship it", 1_001),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', '', '/work/needle', ?1, ?2, ?3, 'text', ?4, ?1)",
                rusqlite::params![uid, ts, role, text],
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
        assert_eq!(rows.len(), 2, "rows: {rows:?}");
    }

    #[test]
    fn a_mirror_id_containing_a_unit_separator_still_links() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: Some("/work/needle".into()),
                prompt: "ship it".into(),
                prompt_hash: Some("hash-sep-mirror".into()),
                timestamp_ms: 1_000,
            },
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'id\u{1f}with\u{1f}sep', 1000, 'user', 'text', 'ship it', 'e-sep')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, message_id, parent_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', '/work/needle', 'm-echo', 'id\u{1f}with\u{1f}sep', 2001, 'assistant', 'text', 'assistant: ship it', 'e-echo')",
            [],
        )
        .unwrap();
        let rows = search_all(
            &conn,
            &["ship".to_string()],
            false,
            &filter(100),
            SearchRole::All,
        )
        .unwrap();
        let prompt_row = rows
            .iter()
            .find(|row| row.match_source == MATCH_SOURCE_HISTORY)
            .expect("the history prompt");
        assert_eq!(prompt_row.echo_count, 1, "rows: {rows:?}");
    }
}
