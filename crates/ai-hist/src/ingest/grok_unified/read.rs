use super::*;

pub(super) fn read_unified(
    conn: &Connection,
    grok_home: &Path,
    only_session: Option<&str>,
) -> Result<GrokUnifiedPass> {
    let mut pass = GrokUnifiedPass::default();
    let path = grok_unified_log_path(grok_home);
    if !grok_unified_log_exists(&path)? {
        return Ok(pass);
    }
    let locator = path.to_string_lossy().to_string();
    let key = transcript_cursor::CursorKey::Locator {
        source: "grok",
        locator: &locator,
    };
    let mut cursor = transcript_cursor::load_cursor(conn, &key)?;
    let saved = if only_session.is_some() {
        None
    } else {
        saved_unified_cursor(&cursor)
    };
    let mut reader = transcript_cursor::TranscriptReader::open(&path, saved, None)
        .with_context(|| format!("read Grok unified log {}", path.display()))?;
    let start = reader.start_offset();
    // Which model each process last named. Only meaningful for the bytes
    // after the cursor, so a log read from the start begins with none.
    let mut pid_models: HashMap<i64, String> = if start == 0 {
        HashMap::new()
    } else {
        cursor
            .extra
            .get(GROK_UNIFIED_CURSOR_KEY)
            .and_then(|state| state.get("pid_models"))
            .and_then(|models| serde_json::from_value(models.clone()).ok())
            .unwrap_or_default()
    };
    let mut touched: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut line = String::new();
    loop {
        check_capture_cancelled()?;
        let offset = reader.position();
        match reader.next_line(&mut line)? {
            None | Some(transcript_cursor::ReadRecord::Unterminated) => break,
            Some(transcript_cursor::ReadRecord::Oversized { .. }) => {
                pass.malformed += 1;
                continue;
            }
            Some(transcript_cursor::ReadRecord::Terminated) => {}
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            pass.malformed += 1;
            continue;
        };
        match grok::parse_unified_row(&value, &pid_models) {
            grok::GrokUnifiedRow::Usage(usage)
                if only_session.is_none_or(|id| id == usage.session_id) =>
            {
                pass.usage_rows += 1;
                pass.new_rows += insert_grok_unified_usage(conn, &value, &usage, &locator, offset)?;
                touched.insert(usage.session_id);
            }
            grok::GrokUnifiedRow::Usage(_) => {}
            grok::GrokUnifiedRow::ModelChange { pid, model } => {
                pid_models.insert(pid, model);
            }
            grok::GrokUnifiedRow::Unattributed => pass.unattributed += 1,
            grok::GrokUnifiedRow::Other => {}
        }
    }
    pass.bytes_read = reader.position().saturating_sub(start);
    pass.cursor_committed = commit_position(
        conn,
        &key,
        &mut cursor,
        reader.commit(reader.position())?,
        pid_models,
        only_session.is_none(),
    )?;
    pass.sessions_materialized = materialize_touched(conn, touched)?;
    let orphans: i64 = conn.query_row(
        "SELECT COUNT(*) FROM grok_unified_usage u WHERE NOT EXISTS \
         (SELECT 1 FROM sessions s WHERE s.source = 'grok' AND s.session_id = u.session_id)",
        [],
        |row| row.get(0),
    )?;
    pass.orphan_rows = orphans as usize;
    Ok(pass)
}

fn commit_position(
    conn: &Connection,
    key: &transcript_cursor::CursorKey<'_>,
    cursor: &mut transcript_cursor::TranscriptCursorState,
    outcome: transcript_cursor::CommitOutcome,
    pid_models: HashMap<i64, String>,
    publish: bool,
) -> Result<bool> {
    let transcript_cursor::CommitOutcome::Published(file) = outcome else {
        return Ok(false);
    };
    if publish {
        cursor.file = Some(file);
        cursor.extra.insert(
            GROK_UNIFIED_CURSOR_KEY.to_string(),
            json!({"pid_models":pid_models,"row_shape":GROK_UNIFIED_ROW_SHAPE}),
        );
        transcript_cursor::store_cursor(conn, key, cursor)?;
    }
    Ok(true)
}
