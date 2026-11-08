//! Echo grouping and the bounded fetch that feeds it: which fetched rows
//! collapse into a primary, how far the fetch grows before a page is
//! provably complete, and the cursor-independent echo recount.

use super::{search_event_rows, search_history_rows, SearchRow, SearchRole, MAX_GROUP_FETCH};
use crate::QueryFilter;
use anyhow::Result;
use rusqlite::Connection;

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
/// trimmed text verbatim (case-sensitive). An explicit id link always wins
/// over a text match, whatever the rows' order; a text-only echo joins the
/// closest preceding primary in its session (the prompt that most recently
/// preceded it, not a later re-utterance of the same words), falling back to
/// the closest following primary when none precedes it. A user or history
/// row never joins another as an echo, so two identical-wording approvals
/// at different timestamps both stay primaries. There is no fuzzy
/// similarity. A row that joins nothing stays as its own singleton group.
/// Only group representatives are returned, with
/// `group_size`/`echo_count` counting the echoes; `bound_hit` marks the
/// oldest group `group_truncated` because the bounded over-fetch may have
/// cut its remaining echoes.
struct Group {
    representative: SearchRow,
    position: usize,
    size: i64,
    absorbable: bool,
}

/// Whether `echo` repeats `primary`'s full trimmed text verbatim.
fn text_matches(echo: &SearchRow, primary: &SearchRow) -> bool {
    let primary_text = collapse_whitespace(primary.text.trim());
    !primary_text.is_empty()
        && (echo.kind == "tool_use" || echo.kind == "tool_result" || echo.role == "assistant")
        && collapse_whitespace(&echo.text).contains(&primary_text)
}

/// Among the primaries an echo could join, the closest preceding one (the
/// greatest `(timestamp_ms, id)` not past the echo) wins; when none
/// precedes it, the closest following one does.
fn pick_closest_preceding(
    candidates: &[usize],
    groups: &[Group],
    echo: &SearchRow,
) -> Option<usize> {
    let key = |index: &usize| {
        let primary = &groups[*index].representative;
        (primary.timestamp_ms, primary.id)
    };
    candidates
        .iter()
        .copied()
        .filter(|index| key(index) <= (echo.timestamp_ms, echo.id))
        .max_by_key(key)
        .or_else(|| candidates.iter().copied().min_by_key(key))
}

/// The group an echo joins, if any: an explicit id link beats every text
/// match, regardless of the rows' order.
fn join_target(groups: &[Group], row: &SearchRow) -> Option<usize> {
    let mut linked: Vec<usize> = Vec::new();
    let mut by_text: Vec<usize> = Vec::new();
    for (index, group) in groups.iter().enumerate() {
        if !group.absorbable
            || group.representative.source != row.source
            || group.representative.session_id != row.session_id
        {
            continue;
        }
        if linked_to(row, &group.representative) {
            linked.push(index);
        } else if text_matches(row, &group.representative) {
            by_text.push(index);
        }
    }
    pick_closest_preceding(&linked, groups, row)
        .or_else(|| pick_closest_preceding(&by_text, groups, row))
}

pub(super) fn group_rows(rows: Vec<SearchRow>, bound_hit: bool) -> Vec<SearchRow> {
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
        let joined = join_target(&groups, &row);
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

/// One fetch of both branches at `fetch_limit` rows each, merged in the
/// global `(timestamp_ms DESC, id DESC, match_source ASC)` order, with the
/// per-branch facts the grouping loop needs to tell a complete page from a
/// bound-cut one.
pub(super) struct FetchedRows {
pub(super)     rows: Vec<SearchRow>,
pub(super)     bound_hit: bool,
pub(super)     history_active: bool,
pub(super)     history_bound_hit: bool,
pub(super)     history_oldest: Option<(i64, i64, String)>,
pub(super)     event_active: bool,
pub(super)     event_bound_hit: bool,
pub(super)     event_oldest: Option<(i64, i64, String)>,
}

fn order_key(row: &SearchRow) -> (i64, i64, String) {
    (row.timestamp_ms, row.id, row.match_source.clone())
}

/// Whether key `a` sorts after (is older than) key `b` in the global order.
fn key_is_after(a: &(i64, i64, String), b: &(i64, i64, String)) -> bool {
    a.0 < b.0 || (a.0 == b.0 && (a.1 < b.1 || (a.1 == b.1 && a.2 > b.2)))
}

pub(super) fn fetch_rows(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
    fetch_limit: i64,
) -> Result<FetchedRows> {
    let fetch_filter = QueryFilter {
        limit: fetch_limit,
        ..filter.clone()
    };
    let mut fetched = FetchedRows {
        rows: Vec::new(),
        bound_hit: false,
        history_active: !matches!(role, SearchRole::Assistant),
        history_bound_hit: false,
        history_oldest: None,
        event_active: !matches!(role, SearchRole::Prompt),
        event_bound_hit: false,
        event_oldest: None,
    };
    if fetched.history_active {
        let branch = search_history_rows(conn, terms, raw_fts, &fetch_filter)?;
        fetched.history_bound_hit = branch.len() as i64 >= fetch_limit;
        fetched.history_oldest = branch.last().map(order_key);
        fetched.bound_hit |= fetched.history_bound_hit;
        fetched.rows.extend(branch);
    }
    if fetched.event_active {
        let branch = search_event_rows(conn, terms, raw_fts, &fetch_filter, role)?;
        fetched.event_bound_hit = branch.len() as i64 >= fetch_limit;
        fetched.event_oldest = branch.last().map(order_key);
        fetched.bound_hit |= fetched.event_bound_hit;
        fetched.rows.extend(branch);
    }
    fetched.rows.sort_by(|a, b| {
        b.timestamp_ms
            .cmp(&a.timestamp_ms)
            .then_with(|| b.id.cmp(&a.id))
            .then_with(|| a.match_source.cmp(&b.match_source))
    });
    Ok(fetched)
}

/// Whether the first `requested` groups (plus the one past them) can no
/// longer change however much older the fetch grows: there is a group past
/// the page, no group in or past the page is a standalone echo whose
/// primary might still sit beyond the fetch bound, and every branch's
/// fetched prefix already reaches the group past the page.
pub(super) fn grouped_page_is_stable(
    grouped: &[SearchRow],
    requested: i64,
    filter: &QueryFilter,
    role: SearchRole,
    fetched: &FetchedRows,
) -> bool {
    let need = requested as usize;
    if grouped.len() <= need {
        return false;
    }
    // A search that can return no primary candidate (an assistant-only
    // role, or a kinds filter naming neither `history` nor `text`) has no
    // grouping to wait for: every row stays its own group.
    let primaries_possible = match role {
        SearchRole::Assistant => false,
        SearchRole::Prompt => true,
        SearchRole::All | SearchRole::User => {
            filter.kinds.is_empty()
                || filter
                    .kinds
                    .iter()
                    .any(|kind| kind == "history" || kind == "text")
        }
    };
    if primaries_possible
        && grouped[..=need]
            .iter()
            .any(|row| !row.is_primary_candidate())
    {
        return false;
    }
    let buffer_key = order_key(&grouped[need]);
    let covers = |active: bool, bound_hit: bool, oldest: &Option<(i64, i64, String)>| {
        !active
            || !bound_hit
            || matches!(oldest, Some(key) if *key == buffer_key || key_is_after(key, &buffer_key))
    };
    covers(
        fetched.history_active,
        fetched.history_bound_hit,
        &fetched.history_oldest,
    ) && covers(
        fetched.event_active,
        fetched.event_bound_hit,
        &fetched.event_oldest,
    )
}

/// Recount each selected primary's echoes over the whole filtered match
/// set, ignoring the page cursor and limit, so `group_size`/`echo_count`
/// do not depend on where the cursor fell. The recount groups every match
/// (up to [`MAX_GROUP_FETCH`] rows per branch) with the same precedence as
/// the page itself; when that bound cuts the recount, a primary it could
/// not recount keeps `group_truncated` set.
pub(super) fn recount_echoes(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
    primaries: &mut [SearchRow],
) -> Result<()> {
    if primaries.is_empty() || matches!(role, SearchRole::Assistant | SearchRole::Prompt) {
        return Ok(());
    }
    let mut universe_filter = filter.clone();
    universe_filter.after = None;
    let fetched = fetch_rows(conn, terms, raw_fts, &universe_filter, role, MAX_GROUP_FETCH)?;
    let bound_hit = fetched.bound_hit;
    let grouped = group_rows(fetched.rows, bound_hit);
    let counts: std::collections::HashMap<(String, i64), (i64, i64, bool)> = grouped
        .iter()
        .map(|row| {
            (
                (row.match_source.clone(), row.id),
                (row.group_size, row.echo_count, row.group_truncated),
            )
        })
        .collect();
    for row in primaries.iter_mut() {
        match counts.get(&(row.match_source.clone(), row.id)) {
            Some(&(size, echoes, truncated)) => {
                row.group_size = size;
                row.echo_count = echoes;
                row.group_id = (size > 1).then(|| format!("{}:{}", row.match_source, row.id));
                row.group_truncated = truncated && bound_hit;
            }
            None if bound_hit => row.group_truncated = true,
            None => {}
        }
    }
    Ok(())
}
