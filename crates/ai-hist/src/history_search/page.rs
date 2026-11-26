//! One search window: fetch, grouping, echo resolution, presentation.

use super::grouping::{
    fetch_rows, group_rows, grouped_page_is_stable, key_is_after, order_key, recount_echoes,
    FetchedRows,
};
use super::presentation::{compute_snippet, DEFAULT_SNIPPET_CHARS};
use super::{
    bounded_fetch_limit, derive_origin, HistoryCursor, QueryFilter, SearchRole, SearchRow,
    GROUP_FETCH_GROWTH_CAP,
};
use anyhow::Result;
use rusqlite::Connection;

pub(super) fn row_cursor(row: &SearchRow) -> HistoryCursor {
    HistoryCursor {
        timestamp_ms: row.timestamp_ms,
        id: row.id,
        match_source: Some(row.match_source.clone()),
    }
}

/// Accept a window that reached [`GROUP_FETCH_GROWTH_CAP`]: keep only grouped
/// rows covered by every capped branch (not older than
/// [`FetchedRows::cap_frontier`]) so pagination never advances past unfetched
/// matches in a capped branch. When every row down to the frontier collapsed
/// into an older primary, returns an empty list with the frontier cursor so
/// the caller continues from that boundary.
fn accept_capped_window(
    mut grouped: Vec<SearchRow>,
    fetched: &FetchedRows,
    requested: i64,
) -> (Vec<SearchRow>, bool, Option<HistoryCursor>) {
    let frontier = fetched.cap_frontier();
    if let Some(ref frontier) = frontier {
        grouped.retain(|row| !key_is_after(&order_key(row), frontier));
    }
    if grouped.is_empty() {
        let cursor = frontier.map(|(timestamp_ms, id, match_source)| HistoryCursor {
            timestamp_ms,
            id,
            match_source: Some(match_source),
        });
        return (grouped, false, cursor);
    }
    for row in grouped.iter_mut().take(requested as usize) {
        if !row.is_primary_candidate() {
            row.group_truncated = true;
        }
    }
    if let Some(last) = grouped.iter_mut().take(requested as usize).last() {
        last.page_incomplete = true;
    }
    (grouped, false, None)
}

/// Fetch and group one window, growing the per-branch bound until the page
/// is provably complete or the growth cap accepts it. Returns the grouped
/// rows, whether the page was proven complete, and an optional continuation
/// cursor when trimming to the capped branch frontier emptied the window.
pub(super) fn fetch_grouped_window(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    window: &QueryFilter,
    role: SearchRole,
    requested: i64,
) -> Result<(Vec<SearchRow>, bool, Option<HistoryCursor>)> {
    let mut fetch_limit = bounded_fetch_limit(requested);
    loop {
        let mut fetched = fetch_rows(conn, terms, raw_fts, window, role, fetch_limit)?;
        let bound_hit = fetched.bound_hit;
        let rows = std::mem::take(&mut fetched.rows);
        let grouped = group_rows(rows, bound_hit);
        if !bound_hit || grouped_page_is_stable(&grouped, requested, window, role, &fetched) {
            return Ok((grouped, true, None));
        }
        if fetch_limit >= GROUP_FETCH_GROWTH_CAP {
            return Ok(accept_capped_window(grouped, &fetched, requested));
        }
        fetch_limit = fetch_limit
            .saturating_mul(2)
            .min(GROUP_FETCH_GROWTH_CAP)
            .max(fetch_limit + 1);
    }
}

/// Resolve one window's echo counts and membership against the un-cursored
/// match set. When the window was left incomplete by the growth cap and
/// resolution dropped every row, returns the cursor past the oldest dropped
/// row so the caller can continue instead of reporting an empty last page.
pub(super) fn resolve_echoes(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    window: &QueryFilter,
    role: SearchRole,
    rows: Vec<SearchRow>,
    page_stable: bool,
) -> Result<(Vec<SearchRow>, Option<HistoryCursor>)> {
    let last_fetched = rows.last().map(row_cursor);
    let mut rows = recount_echoes(conn, terms, raw_fts, window, role, rows)?;
    if page_stable {
        return Ok((rows, None));
    }
    match rows.last_mut() {
        Some(last) => {
            last.page_incomplete = true;
            Ok((rows, None))
        }
        None => Ok((rows, last_fetched)),
    }
}

/// Walk past empty capped windows (up to `max_continuations` steps) and
/// return the grouped, presented rows plus any pending continuation cursor
/// if the continuation cap was hit while more matches may still follow.
pub(super) fn search_window_with_cursor(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
    max_continuations: usize,
) -> Result<(Vec<SearchRow>, Option<HistoryCursor>)> {
    filter.validate()?;
    let requested = filter.limit.max(1);
    let mut window = filter.clone();
    let mut continuations = 0;
    let mut grouped: Vec<SearchRow>;
    let mut pending_cursor: Option<HistoryCursor> = None;
    'pages: loop {
        let (rows, page_stable, window_continuation) =
            fetch_grouped_window(conn, terms, raw_fts, &window, role, requested)?;
        let (rows, continuation) = if let Some(cursor) = window_continuation {
            (rows, Some(cursor))
        } else if window.after.is_none() {
            (rows, None)
        } else {
            resolve_echoes(conn, terms, raw_fts, &window, role, rows, page_stable)?
        };
        grouped = rows;
        match continuation {
            Some(cursor) if continuations < max_continuations => {
                continuations += 1;
                window.after = Some(cursor);
                continue 'pages;
            }
            Some(cursor) => {
                pending_cursor = Some(cursor);
                break;
            }
            None => break,
        }
    }
    grouped.truncate(requested as usize);
    present_rows(&mut grouped, terms, filter);
    Ok((grouped, pending_cursor))
}

/// Fill each returned row's origin and match-centered snippet.
pub(super) fn present_rows(grouped: &mut [SearchRow], terms: &[String], filter: &QueryFilter) {
    let max_chars = filter
        .max_content_chars
        .map(|value| value as usize)
        .unwrap_or(DEFAULT_SNIPPET_CHARS);
    for row in grouped {
        row.origin = derive_origin(&row.match_source, &row.role, row.control_kind.as_deref());
        let (snippet, truncated, offset) = compute_snippet(&row.text, terms, max_chars);
        row.snippet = snippet;
        row.content_truncated = truncated;
        row.snippet_start_offset = offset;
    }
}

