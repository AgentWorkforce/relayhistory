//! The SQL predicates a search adds on each branch: session, kind and
//! origin restrictions, plus the shared row, scope and tag filters.

use super::SearchRole;
use crate::{append_window_filters, normalize_tag_name, QueryFilter, SessionScope};

pub(super) fn append_history_search_filters(
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
                // NULL-safe form of `derive_origin`'s fallthrough: the plain
                // `NOT (... OR control_kind IN (...))` is NULL (and so drops
                // the row) for assistant/tool rows whose `control_kind` is
                // NULL, which `derive_origin` labels `unknown`. The other
                // arms need no such guard: `IS NULL`, `= 'synthetic'` and
                // `IN (...)` each already exclude exactly the rows
                // `derive_origin` labels otherwise.
                "unknown" => sql.push_str(&format!(
                    " AND COALESCE({alias}.control_kind, '') NOT IN ('synthetic','system_reminder','task_notification') \
                     AND NOT ({alias}.control_kind IS NULL AND {alias}.role = 'user')"
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
pub(super) const RUST_WHITESPACE: &str = "char(9, 10, 11, 12, 13, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, \
     8196, 8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288)";

pub(super) fn append_event_search_filters(
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
