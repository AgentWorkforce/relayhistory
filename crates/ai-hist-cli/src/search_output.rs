//! Rendering of `search` results: the JSON rows machine consumers read
//! and the one-line human form.

use ai_hist::history_search::SearchRow;
use anyhow::Result;
use chrono::{Local, TimeZone};
use serde_json::json;

pub(crate) fn print_search_rows(rows: Vec<SearchRow>, as_json: bool) -> Result<()> {
    if as_json {
        let out = rows
            .iter()
            .map(|row| {
                // Provenance is always present: `id` is only unique within
                // its `match_source` table, so a consumer comparing results
                // across surfaces needs the pair.
                json!({
                    "id": row.id,
                    "source": row.source,
                    "session_id": row.session_id,
                    "project": row.project,
                    "prompt": row.text,
                    "timestamp_ms": row.timestamp_ms,
                    "role": row.role,
                    "kind": row.kind,
                    "match_source": row.match_source,
                    "snippet": row.snippet,
                    "content_truncated": row.content_truncated,
                    "snippet_start_offset": row.snippet_start_offset,
                    "group_id": row.group_id,
                    "group_size": row.group_size,
                    "echo_count": row.echo_count,
                    "group_truncated": row.group_truncated,
                    "is_primary": row.is_primary,
                    "origin": row.origin,
                    "control_kind": row.control_kind,
                    "event_uid": row.event_uid,
                    "event_ref": {
                        "match_source": row.match_source,
                        "id": row.id,
                        "session_id": row.session_id,
                        "event_uid": row.event_uid,
                    },
                })
            })
            .collect::<Vec<_>>();
        println!("{}", serde_json::to_string(&out)?);
        return Ok(());
    }
    for row in rows {
        println!("{}", fmt_search_row(&row));
    }
    Ok(())
}

fn fmt_search_row(row: &SearchRow) -> String {
    let dt = Local
        .timestamp_millis_opt(row.timestamp_ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "1970-01-01 00:00".to_string());
    let project = row
        .project
        .as_ref()
        .map(|p| format!(" [{p}]"))
        .unwrap_or_default();
    let label = if row.match_source == "history" {
        row.source.clone()
    } else {
        format!("{}:{}:{}", row.source, row.role, row.kind)
    };
    // The match-centered snippet, already windowed by the search layer.
    let text = row.snippet.replace('\n', " ");
    format!("  #{:<5} {}  ({}){}  {}", row.id, dt, label, project, text)
}
