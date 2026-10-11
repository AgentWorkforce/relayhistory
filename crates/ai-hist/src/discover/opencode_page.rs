//! OpenCode shallow reads: the prompt and model queries, and the paged
//! session enumeration those queries sit beside.

use super::{
    excerpt, opencode_store_holds, OpencodeReadSnapshot, ScanEnv, EXCERPT_MAX_CHARS,
    EXCERPT_TRIM_WHITESPACE,
};
use anyhow::Result;
use rusqlite::{params, OptionalExtension};
use std::collections::BTreeSet;

/// The session's title, unless it is still OpenCode's placeholder. One
/// primary-key lookup on the row enumeration already read.
pub(super) fn opencode_sqlite_title(
    snapshot: &OpencodeReadSnapshot,
    locator: &str,
) -> Result<Option<String>> {
    if !snapshot.session_columns.contains("title") {
        return Ok(None);
    }
    let raw: Option<String> = snapshot
        .conn
        .prepare_cached("SELECT title FROM session WHERE id = ?")?
        .query_row([locator], |row| row.get(0))
        .optional()?
        .flatten();
    Ok(crate::ingest::titles::opencode_title(raw.as_deref()))
}

pub(super) fn opencode_sqlite_prompt(
    scan: &ScanEnv<'_>,
    snapshot: &OpencodeReadSnapshot,
    locator: &str,
) -> Result<Option<String>> {
    let prompt_schema = snapshot.part_columns.contains("data")
        && snapshot.part_columns.contains("message_id")
        && snapshot.message_columns.contains("id")
        && snapshot.message_columns.contains("data");
    if !(prompt_schema
        && (snapshot.part_by_session || (snapshot.message_by_session && snapshot.part_by_message)))
    {
        return Ok(None);
    }
    let keyed_predicate = if snapshot.part_by_session {
        "p.session_id = ?"
    } else {
        "m.session_id = ?"
    };
    let order = match (
        snapshot.part_columns.contains("time_created"),
        snapshot.message_columns.contains("time_created"),
    ) {
        (true, true) => "COALESCE(p.time_created, m.time_created)",
        (true, false) => "p.time_created",
        (false, true) => "m.time_created",
        (false, false) => "p.id",
    };
    let sql = format!(
        "SELECT substr(json_extract(p.data, '$.text'), 1, ?) \
         FROM part p JOIN message m ON m.id = p.message_id \
         WHERE {keyed_predicate} AND json_valid(m.data) AND json_valid(p.data) \
         AND json_extract(m.data, '$.role') = 'user' \
         AND json_extract(p.data, '$.type') = 'text' \
         AND COALESCE(json_type(p.data, '$.synthetic'), 'null') <> 'true' \
         AND json_type(p.data, '$.text') = 'text' \
         AND trim(substr(json_extract(p.data, '$.text'), 1, ?), ?) <> '' \
         ORDER BY {order} ASC LIMIT 1"
    );
    scan.note_query();
    let mut stmt = snapshot.conn.prepare_cached(&sql)?;
    let prompt = stmt
        .query_row(
            params![
                EXCERPT_MAX_CHARS as i64,
                locator,
                EXCERPT_MAX_CHARS as i64,
                EXCERPT_TRIM_WHITESPACE
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    scan.note_records(u64::from(prompt.is_some()));
    Ok(prompt
        .map(|text| excerpt(&text))
        .filter(|text| !text.is_empty()))
}

pub(super) fn opencode_sqlite_model(
    scan: &ScanEnv<'_>,
    snapshot: &OpencodeReadSnapshot,
    locator: &str,
) -> Result<Option<String>> {
    if !(snapshot.message_by_session
        && snapshot.message_columns.contains("session_id")
        && snapshot.message_columns.contains("data"))
    {
        return Ok(None);
    }
    scan.note_query();
    // Match `parse_message` and `OpencodeSession::first_model`: payload time
    // wins, the relational column is its fallback, and a message with neither
    // is not parseable. Checking for a JSON integer mirrors `Value::as_i64`;
    // a string that merely looks numeric must not take precedence here.
    let payload_created = "CASE WHEN json_type(data, '$.time.created') = 'integer' \
                           AND typeof(json_extract(data, '$.time.created')) = 'integer' \
                           THEN json_extract(data, '$.time.created') END";
    let created = if snapshot.message_columns.contains("time_created") {
        format!("COALESCE({payload_created}, time_created)")
    } else {
        payload_created.to_string()
    };
    let order_by = if snapshot.message_columns.contains("id") {
        format!("ORDER BY {created} ASC, id ASC")
    } else {
        format!("ORDER BY {created} ASC")
    };
    let sql = format!(
        "SELECT json_extract(data, '$.providerID'), \
                COALESCE(json_extract(data, '$.modelID'), \
                         json_extract(data, '$.model.modelID')) \
         FROM message WHERE session_id = ? AND json_valid(data) \
         AND json_extract(data, '$.role') = 'assistant' \
         AND {created} IS NOT NULL \
         AND (NULLIF(json_extract(data, '$.providerID'), '') IS NOT NULL \
              OR NULLIF(COALESCE(json_extract(data, '$.modelID'), \
                                 json_extract(data, '$.model.modelID')), '') IS NOT NULL) \
         {order_by} LIMIT 1"
    );
    let mut stmt = snapshot.conn.prepare_cached(&sql)?;
    let model = stmt
        .query_row(params![locator], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        })
        .optional()?
        .and_then(|(provider, model)| {
            crate::ingest::opencode::build_model(provider.as_deref(), model.as_deref())
        });
    scan.note_records(u64::from(model.is_some()));
    Ok(model)
}

pub(super) type OpencodeSessionRow = (String, Option<String>, Option<i64>, Option<i64>);

fn opencode_session_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OpencodeSessionRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
    ))
}

fn earlier_opencode_store_owns(
    scan: &ScanEnv<'_>,
    earlier: &[OpencodeReadSnapshot],
    session_id: &str,
) -> Result<bool> {
    for store in earlier {
        scan.note_query();
        if opencode_store_holds(store, session_id)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn opencode_rows_unpaged(
    scan: &ScanEnv<'_>,
    stmt: &mut rusqlite::Statement<'_>,
    claimed: &BTreeSet<String>,
) -> Result<Vec<OpencodeSessionRow>> {
    scan.note_query();
    let page = stmt
        .query_map([], opencode_session_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    scan.note_records(page.len() as u64);
    Ok(page
        .into_iter()
        .filter(|(id, ..)| !claimed.contains(id))
        .collect())
}

pub(super) fn page_limited_opencode_rows(
    scan: &ScanEnv<'_>,
    stmt: &mut rusqlite::Statement<'_>,
    limit: i64,
    earlier: &[OpencodeReadSnapshot],
    claimed: &BTreeSet<String>,
) -> Result<Vec<OpencodeSessionRow>> {
    // The first page is exactly the limit, so a store that owns all of its
    // newest sessions costs the one query it always did. Only a store whose
    // page lost slots to an earlier store's sessions reads on, in wider pages.
    let mut rows = Vec::new();
    let mut page_size = limit;
    let mut offset: i64 = 0;
    while (rows.len() as i64) < limit && page_size > 0 {
        scan.note_query();
        let page = stmt
            .query_map([page_size, offset], opencode_session_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        scan.note_records(page.len() as u64);
        let exhausted = (page.len() as i64) < page_size;
        offset += page.len() as i64;
        for row in page {
            if (rows.len() as i64) == limit {
                break;
            }
            if claimed.contains(&row.0) {
                continue;
            }
            if !earlier_opencode_store_owns(scan, earlier, &row.0)? {
                rows.push(row);
            }
        }
        if exhausted {
            break;
        }
        page_size = page_size.max(256);
    }
    Ok(rows)
}
