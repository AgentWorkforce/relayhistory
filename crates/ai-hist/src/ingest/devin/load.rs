//! SQLite loads for one Devin session.

use super::{
    numeric_fields, optional_column, parse_json_column, seconds_to_ms, table_columns, DevinNode,
    DevinSessionInfo, DevinToolState,
};
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeMap;

pub(super) fn load_devin_session_info(
    src: &Connection,
    session_id: &str,
) -> Result<Option<DevinSessionInfo>> {
    let columns = table_columns(src, "sessions")?;
    let hidden_pred = if columns.contains("hidden") {
        "COALESCE(hidden, 0) = 0"
    } else {
        "1=1"
    };
    let column = |name| optional_column(&columns, name);
    src.query_row(
        &format!(
            "SELECT id, {}, {}, {}, {}, {}, {}, {}, {}, {} \
             FROM sessions WHERE id = ?1 AND {hidden_pred}",
            column("title"),
            column("working_directory"),
            column("backend_type"),
            column("model"),
            column("agent_mode"),
            column("created_at"),
            column("last_activity_at"),
            column("workspace_dirs"),
            column("metadata"),
        ),
        params![session_id],
        |row| {
            Ok(DevinSessionInfo {
                id: row.get(0)?,
                title: row.get(1)?,
                working_directory: row.get(2)?,
                backend_type: row.get(3)?,
                model: row.get(4)?,
                agent_mode: row.get(5)?,
                created_ms: seconds_to_ms(row.get(6)?),
                last_activity_ms: seconds_to_ms(row.get(7)?),
                workspace_dirs: parse_json_column(row.get(8)?)
                    .and_then(|v| v.as_array().cloned())
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect(),
                metadata: parse_json_column(row.get(9)?)
                    .and_then(|v| v.as_object().cloned())
                    .map(numeric_fields),
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub(super) fn load_devin_nodes(src: &Connection, session_id: &str) -> Result<Vec<DevinNode>> {
    let mut nodes = Vec::new();
    let mut stmt = src.prepare(
        "SELECT node_id, parent_node_id, created_at, chat_message, metadata \
         FROM message_nodes WHERE session_id = ?1 ORDER BY node_id ASC, row_id ASC",
    )?;
    let mut rows = stmt.query(params![session_id])?;
    while let Some(row) = rows.next()? {
        nodes.push(DevinNode {
            node_id: row.get(0)?,
            parent_node_id: row.get(1)?,
            created_ms: seconds_to_ms(row.get(2)?),
            message: parse_json_column(row.get::<_, Option<String>>(3)?),
            node_metadata: parse_json_column(row.get::<_, Option<String>>(4)?),
        });
    }
    Ok(nodes)
}

pub(super) fn load_devin_tools(
    src: &Connection,
    session_id: &str,
) -> Result<BTreeMap<String, DevinToolState>> {
    let mut tools = BTreeMap::new();
    if table_columns(src, "tool_call_state")?.is_empty() {
        return Ok(tools);
    }
    let mut stmt = src.prepare(
        "SELECT tool_call_id, tool_call_json, tool_call_update_json \
         FROM tool_call_state WHERE session_id = ?1 ORDER BY rowid ASC",
    )?;
    let mut rows = stmt.query(params![session_id])?;
    while let Some(row) = rows.next()? {
        tools.insert(
            row.get::<_, String>(0)?,
            DevinToolState {
                call: parse_json_column(row.get::<_, Option<String>>(1)?),
                update: parse_json_column(row.get::<_, Option<String>>(2)?),
            },
        );
    }
    Ok(tools)
}
