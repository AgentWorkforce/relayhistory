use super::*;
pub(super) fn write(conn: &Connection, sid: &str, rows: &[Value]) -> Result<()> {
    let title = resolve(rows);
    conn.execute(
        "UPDATE sessions SET title=?3 WHERE source=?1 AND session_id=?2 AND title IS NOT ?3",
        rusqlite::params![ID, sid, title],
    )?;
    Ok(())
}
pub(super) fn resolve(rows: &[Value]) -> Option<String> {
    let mut facts = Vec::new();
    for row in rows {
        facts.push(row);
        if let Some(timeline) = row["meta"]["codebuddy.ai/sessionTitle"].as_array() {
            facts.extend(timeline);
        }
    }
    facts.sort_by_key(|r| r["timestamp"].as_i64().unwrap_or(0));
    let title = [
        ("custom-title", "customTitle"),
        ("ai-title", "aiTitle"),
        ("topic", "topic"),
    ]
    .into_iter()
    .find_map(|(kind, field)| {
        facts
            .iter()
            .rev()
            .filter(|r| r["type"] == kind)
            .filter_map(|r| r[field].as_str())
            .map(str::trim)
            .find(|s| !s.is_empty() && (kind == "custom-title" || !placeholder(s)))
    });
    title.map(str::to_owned)
}

fn placeholder(s: &str) -> bool {
    s == "(No content)"
        || s == "/compact"
        || (s.starts_with("<image_local_path>") && s.ends_with("</image_local_path>"))
}
