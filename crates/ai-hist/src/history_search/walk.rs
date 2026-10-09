//! The newest-first walk of a branch's timestamp index that answers a search
//! with many matches without sorting all of them; see
//! [`search_branch`](super::search_branch).

use super::{query_branch_rows, BranchMatch, SearchBranch, SearchRow};
use crate::{append_window_filters, raw_fts_query_error, QueryFilter};
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

/// A query matching fewer rows than this is read through its FTS5 index and
/// sorted: the work is bounded by the match count. Counting up to it reads
/// only the head of the term's doclist. Tests shrink this bound and
/// [`RECENT_WINDOW`] so small fixtures cross both.
pub(super) const MATCH_SORT_CAP: i64 = if cfg!(test) { 3 } else { 5_000 };

/// The growing windows a walk reads, in newest rows that pass the time
/// window and cursor; the last is the most it reads before it falls back to
/// sorting every match. See [`search_branch`](super::search_branch).
pub(super) const WALK_WINDOWS: [i64; 3] = if cfg!(test) {
    [2, 3, 6]
} else {
    [2_000, 6_000, 20_000]
};
pub(super) const RECENT_WINDOW: i64 = WALK_WINDOWS[WALK_WINDOWS.len() - 1];

#[cfg(test)]
thread_local! {
    /// Set by tests to read every search through the index-driven plan, the
    /// reference the walk must agree with.
    pub(super) static SORT_EVERY_MATCH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// How many walks returned their rows, and how many fell back.
    pub(super) static WALK_OUTCOMES: std::cell::Cell<(u32, u32)> = const { std::cell::Cell::new((0, 0)) };
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

/// What one branch is searched for, as [`search_branch`](super::search_branch) hands it to each
/// walk window.
pub(super) struct BranchSearch<'a> {
    pub(super) fts_match: Option<&'a BranchMatch>,
    pub(super) filter_sql: &'a str,
    pub(super) filter_params: &'a [String],
    pub(super) filter: &'a QueryFilter,
    pub(super) raw_fts: bool,
}

/// Whether walking the timestamp index can beat sorting every match: the
/// page fits a window, the term matches at least [`MATCH_SORT_CAP`] rows, and
/// the index is there to walk.
pub(super) fn walk_pays(
    conn: &Connection,
    branch: &SearchBranch,
    fts_match: Option<&BranchMatch>,
    limit: i64,
    raw_fts: bool,
) -> Result<bool> {
    // A window smaller than the page could never fill it.
    if !walk_allowed() || limit >= RECENT_WINDOW {
        return Ok(false);
    }
    if let Some(query) = fts_match {
        let fts = branch.fts;
        let matches: i64 = conn
            .prepare_cached(&format!(
                "SELECT count(*) FROM (SELECT 1 FROM {fts} WHERE {fts} MATCH ? LIMIT ?)"
            ))?
            .query_row(params![query.scan, MATCH_SORT_CAP], |row| row.get(0))
            .map_err(|error| raw_fts_query_error(raw_fts, error))?;
        if matches < MATCH_SORT_CAP {
            return Ok(false);
        }
    }
    index_exists(conn, branch.ts_index)
}

/// The first `limit` matches, read by walking the timestamp index in growing
/// [`WALK_WINDOWS`], or `None` when the walk gives up and the search has to
/// sort every match.
pub(super) fn walk(
    conn: &Connection,
    branch: &SearchBranch,
    search: &BranchSearch<'_>,
    limit: i64,
) -> Result<Option<Vec<SearchRow>>> {
    for (step, window) in WALK_WINDOWS.iter().copied().enumerate() {
        let floor = recent_window_floor(conn, branch, search.filter, window)?;
        // Rows tied on the floor's timestamp all join the window, and the
        // walk tests every one to order the tie by id. A tie as large as
        // the window itself (a bulk import stamped with one time) would
        // make the walk cost more than sorting every match, so fall back.
        if let Some(floor) = floor {
            if tie_reaches(conn, branch, search.filter, floor, window)? {
                break;
            }
        }
        // The tie check bounds this window's rows at or above its floor to
        // fewer than twice the window, and each window in WALK_WINDOWS is
        // larger than that, so every step's floor is strictly older than
        // the last and each window is read once.
        let rows = walk_window(conn, branch, search, floor)?;
        // A window that holds every eligible row is the whole search.
        if floor.is_none() || rows.len() as i64 >= limit {
            record_walk(true);
            return Ok(Some(rows));
        }
        if !worth_growing(rows.len() as i64, step, window, limit) {
            break;
        }
    }
    record_walk(false);
    Ok(None)
}

/// Whether a window that found `found` matches should grow while it could
/// still fill the page: matches at a rate that could fill the last window
/// (with 4x slack, as they cluster by session), or none yet in only the
/// first, which a fresh session without the term can account for. Two
/// windows with no match -- a filter nothing recent passes, a term the newest
/// rows do not use -- fall back after a few thousand rows.
fn worth_growing(found: i64, step: usize, window: i64, limit: i64) -> bool {
    if found == 0 {
        step == 0
    } else {
        found * (RECENT_WINDOW / window) * 4 >= limit
    }
}

/// Whether an index exists. `init_db` creates both timestamp indexes, but
/// neither is a schema-currency requirement, so a read-only handle on an
/// older database may lack one and `INDEXED BY` would fail to prepare.
fn index_exists(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn
        .prepare_cached("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?")?
        .exists([name])?)
}

/// The branch's matches, at most `limit` of them, among the rows at or after
/// `floor` that pass the time window and cursor (every such row when `floor`
/// is `None`). See [`recent_window_floor`].
///
/// The window is every row at or after the `window`-th row's timestamp: a
/// prefix of the `(timestamp DESC, id DESC)` order, so `limit` matches found
/// in it are the search's first `limit`. Rows tied with that timestamp extend
/// it past `window`; breaking the tie by id would sort the whole tie.
fn walk_window(
    conn: &Connection,
    branch: &SearchBranch,
    search: &BranchSearch<'_>,
    floor: Option<i64>,
) -> Result<Vec<SearchRow>> {
    let &BranchSearch {
        fts_match,
        filter_sql,
        filter_params,
        filter,
        raw_fts,
    } = search;
    let SearchBranch {
        table,
        alias,
        fts,
        ts_column,
        ts_index,
        columns,
        ..
    } = branch;
    // The walk orders ids only: rows tied on the floor's timestamp are sorted
    // by id, and a sorter of ids is cheap where one of full rows (event text
    // included) is not. The page's rows are read once it is chosen.
    let mut sql = format!(
        "SELECT {columns} FROM {table} {alias} WHERE {alias}.id IN (\
         SELECT {alias}.id FROM {table} {alias} INDEXED BY {ts_index} WHERE 1=1"
    );
    let mut params_vec = Vec::new();
    if let Some(query) = fts_match {
        sql.push_str(&format!(
            " AND EXISTS (SELECT 1 FROM {fts} f WHERE {fts} MATCH ? AND f.rowid = {alias}.id)"
        ));
        params_vec.push(query.probe.clone());
    }
    if let Some(floor) = floor {
        sql.push_str(&format!(" AND {alias}.{ts_column} >= ?"));
        params_vec.push(floor.to_string());
    }
    sql.push_str(filter_sql);
    params_vec.extend(filter_params.iter().cloned());
    sql.push_str(&format!(
        " ORDER BY {alias}.{ts_column} DESC, {alias}.id DESC LIMIT ?) \
         ORDER BY {alias}.{ts_column} DESC, {alias}.id DESC"
    ));
    params_vec.push(filter.limit.max(1).to_string());
    query_branch_rows(conn, branch, &sql, params_vec, raw_fts)
}

/// Whether at least `window` rows that pass the search's time window and
/// cursor share timestamp `ts`: an index-only count that stops at `window`.
/// Rows a cursor already passed are not counted, so a deep page inside a large
/// tie walks only the tied rows it still has to order.
fn tie_reaches(
    conn: &Connection,
    branch: &SearchBranch,
    filter: &QueryFilter,
    ts: i64,
    window: i64,
) -> Result<bool> {
    let column = format!("{}.{}", branch.alias, branch.ts_column);
    let (mut sql, mut params_vec) = eligible_rows(branch, filter, &column);
    sql.push_str(&format!(" AND {column} = ?"));
    params_vec.push(ts.to_string());
    let tied: i64 = conn
        .prepare(&format!("SELECT count(*) FROM ({sql} LIMIT ?)"))?
        .query_row(
            rusqlite::params_from_iter(params_vec.into_iter().chain([window.to_string()])),
            |row| row.get(0),
        )?;
    Ok(tied >= window)
}

/// The timestamp of the `window`-th newest row that passes the
/// search's time window and cursor, or `None` when fewer rows pass.
fn recent_window_floor(
    conn: &Connection,
    branch: &SearchBranch,
    filter: &QueryFilter,
    window: i64,
) -> Result<Option<i64>> {
    let column = format!("{}.{}", branch.alias, branch.ts_column);
    let (mut sql, mut params_vec) = eligible_rows(branch, filter, &column);
    sql.push_str(&format!(" ORDER BY {column} DESC LIMIT 1 OFFSET ?"));
    params_vec.push((window - 1).to_string());
    Ok(conn
        .prepare(&sql)?
        .query_row(rusqlite::params_from_iter(params_vec), |row| row.get(0))
        .optional()?)
}

/// `SELECT <ts>` over the branch's timestamp index, restricted to the rows
/// that pass the search's time window and cursor.
fn eligible_rows(
    branch: &SearchBranch,
    filter: &QueryFilter,
    column: &str,
) -> (String, Vec<String>) {
    let SearchBranch {
        table,
        alias,
        ts_index,
        after_history_tie,
        ..
    } = branch;
    let mut sql = format!("SELECT {column} FROM {table} {alias} INDEXED BY {ts_index} WHERE 1=1");
    let mut params_vec = Vec::new();
    if let Some(before_ms) = filter.before_ms {
        sql.push_str(&format!(" AND {column} < ?"));
        params_vec.push(before_ms.to_string());
    }
    append_window_filters(
        &mut sql,
        &mut params_vec,
        filter,
        column,
        &format!("{alias}.id"),
        *after_history_tie,
    );
    (sql, params_vec)
}
