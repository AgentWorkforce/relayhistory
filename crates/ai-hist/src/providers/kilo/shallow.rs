use super::*;
use anyhow::ensure;

pub(super) fn first_prompt(conn: &Connection, native: &str) -> Result<Option<String>> {
    const MAX_SHALLOW_ROWS: usize = 256;
    const MAX_SHALLOW_BYTES: i64 = 16 * 1024 * 1024;
    let max_message_bytes: Option<i64> = conn.query_row(
        "SELECT MAX(length(CAST(data AS BLOB))) FROM message WHERE session_id=?1",
        [native],
        |row| row.get(0),
    )?;
    ensure!(
        max_message_bytes.unwrap_or(0) <= storage::MAX_ROW_BYTES,
        "oversized Kilo prompt message data"
    );
    let mut stmt = conn.prepare("SELECT CASE WHEN length(CAST(m.id AS BLOB))<=512 THEN m.id ELSE '' END,CASE WHEN length(CAST(m.data AS BLOB))<=?2 THEN json_extract(m.data,'$.summary') ELSE NULL END,CASE WHEN length(CAST(p.data AS BLOB))<=?2 THEN p.data ELSE '' END,length(CAST(p.data AS BLOB)) FROM part p JOIN message m ON m.id=p.message_id WHERE m.session_id=?1 AND CASE WHEN length(CAST(m.data AS BLOB))<=?2 THEN json_extract(m.data,'$.role') ELSE NULL END='user' ORDER BY m.time_created,m.id,p.id LIMIT ?3")?;
    let mut rows = stmt.query(rusqlite::params![
        native,
        storage::MAX_ROW_BYTES,
        (MAX_SHALLOW_ROWS + 1) as i64
    ])?;
    let mut message_id = String::new();
    let mut prompt = String::new();
    let mut chars = 0;
    let mut row_count = 0;
    let mut byte_count = 0;
    while let Some(row) = rows.next()? {
        crate::ingest::check_capture_cancelled()?;
        let id: String = row.get(0)?;
        let summary: Option<i64> = row.get(1)?;
        let raw: String = row.get(2)?;
        let bytes: i64 = row.get(3)?;
        row_count += 1;
        byte_count += bytes;
        ensure!(
            row_count <= MAX_SHALLOW_ROWS && byte_count <= MAX_SHALLOW_BYTES,
            "Kilo shallow prompt exceeds bounded scan budget"
        );
        ensure!(
            !id.is_empty() && bytes <= storage::MAX_ROW_BYTES,
            "oversized Kilo shallow prompt row"
        );
        if id != message_id {
            if !prompt.is_empty() {
                return Ok(Some(prompt));
            }
            message_id = id;
        }
        if summary == Some(1) {
            continue;
        }
        let data: Value = serde_json::from_str(&raw)?;
        if data["type"] != "text" || data["synthetic"] == true || data["ignored"] == true {
            continue;
        }
        let Some(text) = data["text"].as_str() else {
            continue;
        };
        if !prompt.is_empty() && chars < 512 {
            prompt.push('\n');
            chars += 1;
        }
        let fragment: String = text.chars().take(512 - chars).collect();
        chars += fragment.chars().count();
        prompt.push_str(&fragment);
        if chars == 512 {
            return Ok(Some(prompt));
        }
    }
    Ok((!prompt.is_empty()).then_some(prompt))
}
