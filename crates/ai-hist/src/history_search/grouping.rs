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
/// Only transcript ids compare — never the primary's SQLite row id, which a
/// provider id could collide with by chance.
fn linked_to(candidate: &SearchRow, primary: &SearchRow) -> bool {
    let primary_ids = [
        primary.message_id.as_deref(),
        primary.event_uid.as_deref(),
        primary.tool_use_id.as_deref(),
    ];
    let names_primary = |value: &str| {
        primary_ids.iter().flatten().any(|id| *id == value)
            || primary.alt_ids.iter().any(|id| id == value)
    };
    candidate.parent_id.as_deref().is_some_and(names_primary)
        || candidate.tool_use_id.as_deref().is_some_and(names_primary)
}

/// Collapse fetched rows into groups around primaries, newest first.
///
/// A primary is a `history` row or a `role = user`/`kind = text` event. Any
/// other row joins a primary only in the same source and session when it is
/// a tool call/result or an assistant row whose whitespace-collapsed text
/// starts or ends with the primary's full trimmed text (case-sensitive):
/// verbatim repetition is the echo evidence, and conversation ancestry
/// alone never suppresses a substantive reply. Among the primaries a
/// repeating row could join, an explicit id link wins over a text-only
/// match, whatever the rows' order; a text-only echo joins the closest
/// preceding primary in its session (the prompt that most recently
/// preceded it, not a later re-utterance of the same words), falling back
/// to the closest following primary when none precedes it. A user or
/// history row never joins another as an echo, so two identical-wording
/// approvals at different timestamps both stay primaries. There is no
/// fuzzy similarity. A row that joins nothing stays as its own singleton
/// group. Only group representatives are returned, with
/// `group_size`/`echo_count` counting the echoes; `bound_hit` marks every
/// primary `group_truncated` because a bound-cut fetch may have omitted
/// some of its echoes.
struct Group {
    representative: SearchRow,
    position: usize,
    size: i64,
    absorbable: bool,
    /// The primary's whitespace-collapsed text, computed once: grouping
    /// compares every pending row against every primary, so collapsing on
    /// each comparison would re-allocate the same strings quadratically.
    primary_text: String,
}

/// Whether an echo candidate (by kind/role) mirrors the primary's full
/// trimmed text: the collapsed echo text contains the primary's whole
/// text, and that text makes up at least a third of the echo. Mere
/// containment is not enough — a substantive reply that quotes the prompt
/// in passing ("Do not ship it: the tests failed", "ship it only after
/// fixing these failures") is a match in its own right, while a wrapper
/// that mostly is the prompt ("quoting <prompt> 1999", "assistant:
/// <prompt>") is an echo. The coverage check applies to prefix and suffix
/// occurrences too: starting with the prompt does not make a long reply
/// an echo. Both texts arrive whitespace-collapsed, each collapsed once
/// per search rather than once per comparison; lengths are codepoints,
/// cached by the caller where compared repeatedly.
fn text_matches(echo: &SearchRow, echo_text: &str, primary_text: &str) -> bool {
    if primary_text.is_empty()
        || !(echo.kind == "tool_use" || echo.kind == "tool_result" || echo.role == "assistant")
    {
        return false;
    }
    // A tool payload may serialize the prompt with escaped quotes; that
    // serialization is still the same verbatim text.
    let unescaped;
    let echo_text = if echo_text.contains("\\\"") {
        unescaped = echo_text.replace("\\\"", "\"");
        &unescaped
    } else {
        echo_text
    };
    echo_text.contains(primary_text)
        && primary_text.chars().count() * 3 >= echo_text.chars().count()
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

/// The group an echo joins, if any. A row joins only with evidence that it
/// actually repeats the primary: its text must contain the primary's full
/// text verbatim. Conversation ancestry alone is not echo evidence — an
/// ordinary assistant reply is parented to its prompt without repeating
/// it, and suppressing it would hide a substantive match. Among the
/// primaries a repeating row could join, an explicit id link beats a
/// text-only match, regardless of the rows' order.
fn join_target(groups: &[Group], candidates: &[usize], row: &SearchRow, row_text: &str) -> Option<usize> {
    let mut linked: Vec<usize> = Vec::new();
    let mut by_text: Vec<usize> = Vec::new();
    for &index in candidates {
        let group = &groups[index];
        if !text_matches(row, row_text, &group.primary_text) {
            continue;
        }
        if linked_to(row, &group.representative) {
            linked.push(index);
        } else {
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
    let mut pending: Vec<(usize, SearchRow, String)> = Vec::new();
    for (position, row) in rows.into_iter().enumerate() {
        if row.is_primary_candidate() {
            let primary_text = collapse_whitespace(row.text.trim());
            groups.push(Group {
                representative: row,
                position,
                size: 1,
                absorbable: true,
                primary_text,
            });
        } else {
            let row_text = collapse_whitespace(&row.text);
            pending.push((position, row, row_text));
        }
    }
    // Candidate primaries are bucketed by (source, session): an echo can
    // only join a primary in its own bucket, so matching never scans the
    // primaries of other sessions. Singleton groups stay out of the
    // matching set entirely.
    let mut buckets: std::collections::HashMap<(String, Option<String>), Vec<usize>> =
        std::collections::HashMap::new();
    for (index, group) in groups.iter().enumerate() {
        buckets
            .entry((
                group.representative.source.clone(),
                group.representative.session_id.clone(),
            ))
            .or_default()
            .push(index);
    }
    let mut singletons: Vec<Group> = Vec::new();
    for (position, row, row_text) in pending {
        // Rows with no (or an empty) session id have no proven session to
        // group within: unrelated session-less rows must not hide each
        // other as echoes.
        let candidates: &[usize] = match row.session_id.as_deref() {
            Some(session) if !session.is_empty() => buckets
                .get(&(row.source.clone(), row.session_id.clone()))
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            _ => &[],
        };
        let joined = join_target(&groups, candidates, &row, &row_text);
        match joined {
            Some(index) => groups[index].size += 1,
            None => singletons.push(Group {
                primary_text: String::new(),
                representative: row,
                position,
                size: 1,
                absorbable: false,
            }),
        }
    }
    groups.extend(singletons);
    groups.sort_by_key(|group| group.position);
    groups
        .into_iter()
        .map(|group| {
            let mut row = group.representative;
            row.group_size = group.size;
            row.echo_count = group.size - 1;
            row.group_id = (group.size > 1)
                .then(|| format!("{}:{}", row.match_source, row.id));
            // A bound-cut fetch can omit echoes of any primary in it —
            // the other branch's bound cuts independently — so every
            // primary's count from such a fetch is provisional, not just
            // the oldest group's: a later, larger fetch can also move an
            // echo to its explicitly linked prompt and lower the count.
            row.group_truncated = bound_hit && group.absorbable;
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
/// do not depend on where the cursor fell. An echo can only join a primary
/// in its own session, so the recount is scoped to the sessions of the
/// primaries actually on the page — one bounded fetch and grouping per
/// distinct session on the page, never a regrouping of the whole match
/// corpus per continued page. Primaries with no session id keep the
/// unscoped recount, since their echoes are the session-less rows too.
/// When a recount bound is hit, a primary it could not recount keeps
/// `group_truncated` set.
///
/// The same un-cursored grouping also settles membership across the
/// cursor: a standalone row on this page that the recount shows joined a
/// primary before the cursor (an echo older than its prompt, surfacing
/// only now) is dropped rather than returned a second time as its own
/// match. When anything is dropped, the last kept row is marked
/// `page_incomplete` so the caller still gets a cursor.
pub(super) fn recount_echoes(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
    primaries: Vec<SearchRow>,
) -> Result<Vec<SearchRow>> {
    if primaries.is_empty() || matches!(role, SearchRole::Assistant | SearchRole::Prompt) {
        return Ok(primaries);
    }
    // No primary candidate on the page does not skip resolution: a page of
    // echoes alone (their prompt already returned) must still be resolved
    // against the un-cursored grouping, or the echoes would be returned a
    // second time as their own matches.
    // Distinct sessions among the page's rows, in page order — standalone
    // rows included, since resolving whether one is an absorbed echo needs
    // its session's grouping too. `None` (a session-less row) falls back
    // to one unscoped recount.
    let mut sessions: Vec<Option<String>> = Vec::new();
    for row in primaries.iter() {
        if !sessions.contains(&row.session_id) {
            sessions.push(row.session_id.clone());
        }
    }
    let mut counts: std::collections::HashMap<(String, i64), (i64, i64, bool)> =
        std::collections::HashMap::new();
    let mut session_bound_hit: std::collections::HashMap<Option<String>, bool> =
        std::collections::HashMap::new();
    let mut any_bound_hit = false;
    for session in sessions {
        let mut universe_filter = filter.clone();
        universe_filter.after = None;
        if let Some(session_id) = &session {
            universe_filter.session_id = Some(session_id.clone());
        }
        let fetched = fetch_rows(conn, terms, raw_fts, &universe_filter, role, MAX_GROUP_FETCH)?;
        let bound_hit = fetched.bound_hit;
        any_bound_hit |= bound_hit;
        let unscoped = session.is_none() && filter.session_id.is_none();
        session_bound_hit.insert(session, bound_hit);
        for row in group_rows(fetched.rows, bound_hit) {
            // The session-less recount fetches without a session filter,
            // so it sees every session's rows; only its own session-less
            // results may be recorded, or a bound-cut unscoped grouping
            // could overwrite a complete session recount's membership.
            if unscoped && row.session_id.is_some() {
                continue;
            }
            counts.insert(
                (row.match_source.clone(), row.id),
                (row.group_size, row.echo_count, row.group_truncated),
            );
        }
    }
    Ok(apply_recount(
        primaries,
        &counts,
        &session_bound_hit,
        any_bound_hit,
    ))
}

/// Apply the recount's counts to the page rows and drop the standalone
/// rows it proves are echoes of a primary before the cursor: they were
/// already counted on that primary's page and must not reappear as their
/// own match. A row whose session recount hit its bound is kept — its
/// membership is unproven — and a kept singleton never loses the
/// truncation warning that bound raised.
fn apply_recount(
    mut primaries: Vec<SearchRow>,
    counts: &std::collections::HashMap<(String, i64), (i64, i64, bool)>,
    session_bound_hit: &std::collections::HashMap<Option<String>, bool>,
    any_bound_hit: bool,
) -> Vec<SearchRow> {
    for row in primaries.iter_mut() {
        let session_bound = session_bound_hit
            .get(&row.session_id)
            .copied()
            .unwrap_or(false);
        let page_flagged = row.group_truncated;
        match counts.get(&(row.match_source.clone(), row.id)) {
            Some(&(size, echoes, truncated)) => {
                row.group_size = size;
                row.echo_count = echoes;
                row.group_id = (size > 1).then(|| format!("{}:{}", row.match_source, row.id));
                row.group_truncated = truncated && any_bound_hit;
                if !row.is_primary_candidate() {
                    row.group_truncated = row.group_truncated || page_flagged || session_bound;
                }
            }
            None if any_bound_hit => row.group_truncated = true,
            None => {}
        }
    }
    let before = primaries.len();
    primaries.retain(|row| {
        row.is_primary_candidate()
            || counts.contains_key(&(row.match_source.clone(), row.id))
            || session_bound_hit
                .get(&row.session_id)
                .copied()
                .unwrap_or(false)
    });
    if primaries.len() < before && any_bound_hit {
        // Rows dropped while a recount bound was hit somewhere: the page
        // may not be the last, so keep a continuation signal. With every
        // recount complete, the grouped set was the whole window and a
        // drop implies nothing about older matches.
        if let Some(last) = primaries.last_mut() {
            last.page_incomplete = true;
        }
    }
    primaries
}
