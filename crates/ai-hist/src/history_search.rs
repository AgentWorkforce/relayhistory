//! Typed history and event search, independent of command-line formatting.
//!
//! This is the one search contract every surface uses: the CLI's `search`,
//! the napi `search` the TypeScript SDK wraps, and the MCP `search_history`
//! tool. It matches user prompts (`history`) and normalized session events
//! (`session_events`) through their FTS5 indexes, applies the same
//! scope/source/project/tag/time filters to both, and orders the merged result
//! by `(timestamp_ms DESC, id DESC, match_source)` so a tie never reorders
//! between calls.
use crate::{normalize_tag_name, raw_fts_query_error, QueryFilter, SessionScope};
use anyhow::Result;
use rusqlite::Connection;
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
    sql.push_str(" ORDER BY h.timestamp_ms DESC, h.id DESC LIMIT ?");
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
                match_source: MATCH_SOURCE_HISTORY.to_string(),
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
        // `session_events_fts` also indexes `role`; an ordinary query must not
        // match `assistant` or `user` against it, or searching for the word
        // would return every event of that role. `text` and `project` are the
        // columns `history_fts` matches a prompt on (`prompt`, `project`), so
        // both branches read a term the same way. A raw query is the caller's
        // FTS5 expression, column filters included, and is left as written.
        let query = crate::build_fts_query(terms, raw_fts);
        params_vec.push(if raw_fts {
            query
        } else {
            format!("{{text project}} : ({query})")
        });
        "SELECT e.id, e.source, e.session_id, e.project, COALESCE(e.text, ''), e.ts_ms, e.role, e.kind \
         FROM session_events_fts f JOIN session_events e ON f.rowid = e.id WHERE session_events_fts MATCH ?"
            .to_string()
    };
    // The query the history branch runs, which a prompt must match for its
    // event copy to be dropped as a duplicate of it.
    let prompt_match = (!terms.is_empty()).then(|| crate::build_fts_query(terms, raw_fts));
    append_event_search_filters(
        &mut sql,
        &mut params_vec,
        filter,
        "e",
        role,
        prompt_match.as_deref(),
    );
    sql.push_str(" ORDER BY e.ts_ms DESC, e.id DESC LIMIT ?");
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
                match_source: MATCH_SOURCE_SESSION_EVENT.to_string(),
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
    if let Some(before_ms) = filter.before_ms {
        sql.push_str(&format!(" AND {alias}.timestamp_ms < ?"));
        params.push(before_ms.to_string());
    }
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
}
