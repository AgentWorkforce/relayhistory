//! One search window: fetch, grouping, echo resolution, presentation.

use super::grouping::{fetch_rows, group_rows, grouped_page_is_stable, recount_echoes};
use super::presentation::{compute_snippet, DEFAULT_SNIPPET_CHARS};
use super::{
    bounded_fetch_limit, derive_origin, HistoryCursor, QueryFilter, SearchRole, SearchRow,
    GROUP_FETCH_GROWTH_CAP,
};
use anyhow::Result;
use rusqlite::Connection;

/// Fetch and group one window, growing the per-branch bound until the page
/// is provably complete or the growth cap accepts it. Returns the grouped
/// rows and whether the page was proven complete; when it was not, the
/// possibly-echo rows are flagged and the last row carries the
/// `page_incomplete` marker `search_page` turns into a cursor.
pub(super) fn fetch_grouped_window(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    window: &QueryFilter,
    role: SearchRole,
    requested: i64,
) -> Result<(Vec<SearchRow>, bool)> {
    let mut fetch_limit = bounded_fetch_limit(requested);
    loop {
        let mut fetched = fetch_rows(conn, terms, raw_fts, window, role, fetch_limit)?;
        let bound_hit = fetched.bound_hit;
        let rows = std::mem::take(&mut fetched.rows);
        let mut grouped = group_rows(rows, bound_hit);
        if !bound_hit || grouped_page_is_stable(&grouped, requested, window, role, &fetched) {
            return Ok((grouped, true));
        }
        if fetch_limit >= GROUP_FETCH_GROWTH_CAP {
            for row in grouped.iter_mut().take(requested as usize) {
                if !row.is_primary_candidate() {
                    row.group_truncated = true;
                }
            }
            if let Some(last) = grouped.iter_mut().take(requested as usize).last() {
                last.page_incomplete = true;
            }
            return Ok((grouped, false));
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
    let last_fetched = rows.last().map(|row| HistoryCursor {
        timestamp_ms: row.timestamp_ms,
        id: row.id,
        match_source: Some(row.match_source.clone()),
    });
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

