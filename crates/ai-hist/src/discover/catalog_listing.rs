use super::*;

/// The catalog listing query and its bound arguments.
///
/// Built in one place so the query-plan test asserts the plan of the statement
/// that actually runs.
pub(super) fn catalog_list_query(options: &CatalogListOptions, indexed_project: bool) -> (String, Vec<Box<dyn rusqlite::ToSql>>) {
    // Every filter below is appended as ` AND …`.
    let mut sql = format!("SELECT {SESSION_COLUMNS}");
    let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(project_key) = options.project_key.as_ref().filter(|_| indexed_project) {
        sql.push_str(catalog_project_lookup::FROM_PROJECT);
        args.push(Box::new(project_key.clone()));
        args.push(Box::new(project_key.clone()));
    } else {
        sql.push_str(" FROM sessions");
    }
    sql.push_str(" WHERE 1 = 1");
    match options.scope {
        SessionScope::Local => sql.push_str(
            " AND (EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'local') \
               OR NOT EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id))",
        ),
        SessionScope::Remote => sql.push_str(
            " AND EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'remote')",
        ),
        SessionScope::All => {}
    }
    if !options.sources.is_empty() {
        let placeholders = vec!["?"; options.sources.len()].join(", ");
        sql.push_str(&format!(" AND source IN ({placeholders})"));
        for source in &options.sources {
            args.push(Box::new(source.clone()));
        }
    }
    if let Some(project_key) = options.project_key.as_ref().filter(|_| !indexed_project) {
        sql.push_str(" AND catalog_project_key(source, project_key_method, repo_url, project_key) = ?");
        args.push(Box::new(project_key.clone()));
    }
    if options.exclude_delegated {
        sql.push_str(" AND COALESCE(sessions.session_kind <> 'subagent' OR sessions.source <> 'grok', 1)");
        sql.push_str(&format!(
            " AND NOT {}",
            crate::relationships::delegated_child_sql("sessions.source", "sessions.session_id")
        ));
    }
    match options.after.as_ref() {
        // Everything strictly after the cursor in the catalog's total order.
        // Undated rows sort last, so a dated cursor must still reach them.
        Some(cursor) => match cursor.last_activity_ms {
            Some(ms) => {
                sql.push_str(
                    " AND (last_activity_ms IS NULL OR last_activity_ms < ? \
                       OR (last_activity_ms = ? \
                           AND (source > ? OR (source = ? AND session_id > ?))))",
                );
                args.push(Box::new(ms));
                args.push(Box::new(ms));
                args.push(Box::new(cursor.source.clone()));
                args.push(Box::new(cursor.source.clone()));
                args.push(Box::new(cursor.session_id.clone()));
            }
            None => {
                sql.push_str(
                    " AND last_activity_ms IS NULL \
                       AND (source > ? OR (source = ? AND session_id > ?))",
                );
                args.push(Box::new(cursor.source.clone()));
                args.push(Box::new(cursor.source.clone()));
                args.push(Box::new(cursor.session_id.clone()));
            }
        },
        None => {
            if let Some(before_ms) = options.before_ms {
                sql.push_str(" AND last_activity_ms < ?");
                args.push(Box::new(before_ms));
            }
        }
    }
    sql.push_str(" ORDER BY last_activity_ms DESC, source ASC, session_id ASC LIMIT ?");
    args.push(Box::new(options.limit.unwrap_or(DEFAULT_CATALOG_LIMIT)));
    (sql, args)
}

/// List the session catalog straight out of the database.
///
/// Reads catalog metadata from SQLite. Project filters use the same
/// cache-only projection as the returned rows.
///
/// Rows come back in the catalog's total order:
/// `(last_activity_ms DESC, source ASC, session_id ASC)`, with rows of unknown
/// recency last. Use [`list_session_catalog_page`] to paginate.
pub fn list_session_catalog(
    conn: &Connection,
    options: &CatalogListOptions,
) -> Result<Vec<ShallowSession>> {
    let indexed = prepare_project_filter(conn, options)?;
    let (sql, args) = catalog_list_query(options, indexed);
    let mut stmt = conn.prepare(&sql)?;
    let params = rusqlite::params_from_iter(args.iter().map(|arg| arg.as_ref()));
    let rows = stmt
        .query_map(params, row_to_session)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub(super) fn prepare_project_filter(
    conn: &Connection,
    options: &CatalogListOptions,
) -> Result<bool> {
    if options.project_key.is_none() {
        return Ok(false);
    }
    catalog_row::register_catalog_project_key(conn)?;
    catalog_project_lookup::is_current(conn)
}

/// [`list_session_catalog`] plus the cursor that continues it.
///
/// The cursor is `None` once a page comes back short of its limit, so a
/// caller walks the catalog by following `next_cursor` until it is absent —
/// no duplicated and no skipped rows, even when a whole page shares one
/// millisecond.
pub fn list_session_catalog_page(
    conn: &Connection,
    options: &CatalogListOptions,
) -> Result<SessionCatalogPage> {
    let sessions = list_session_catalog(conn, options)?;
    let limit = options.limit.unwrap_or(DEFAULT_CATALOG_LIMIT);
    let next_cursor = (limit > 0 && sessions.len() as i64 >= limit)
        .then(|| sessions.last())
        .flatten()
        .map(|row| CatalogCursor {
            last_activity_ms: row.last_activity_ms,
            source: row.source.clone(),
            session_id: row.session_id.clone(),
        });
    Ok(SessionCatalogPage {
        scope: options.scope,
        sessions,
        next_cursor,
    })
}
