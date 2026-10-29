//! Complete Claude transcript helpers and source-order conformance.
use super::*;

/// The first in-log session id a transcript carries.
///
/// Read ahead of the record walk, and bounded in the case that matters: the
/// id is normally on the first record, and this stops at the first one that
/// has it.
pub(super) fn claude_file_session_id(path: &Path) -> Result<Option<String>> {
    let file = fs::File::open(path)
        .with_context(|| format!("reading claude transcript {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut raw = Vec::new();
    loop {
        raw.clear();
        if reader.read_until(b'\n', &mut raw)? == 0 {
            return Ok(None);
        }
        let Ok(line) = std::str::from_utf8(&raw) else {
            continue;
        };
        if let Some(id) = serde_json::from_str::<Value>(line.trim_end_matches(['\n', '\r']))
            .ok()
            .as_ref()
            .and_then(|value| value.get("sessionId"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            return Ok(Some(id.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(ids: &[&str]) -> String {
        ids.iter().map(|id| serde_json::json!({"type":"user","uuid":id,"sessionId":"native-claude","timestamp":"2026-10-22T12:00:00Z","message":{"role":"user","content":[{"type":"text","text":id}]}}).to_string()+"\n").collect()
    }
    fn rows(conn: &Connection) -> Vec<(i64, String, i64, String)> {
        conn.prepare("SELECT id,event_uid,ts_ms,text FROM session_events WHERE source='claude' ORDER BY event_uid").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap().collect::<rusqlite::Result<_>>().unwrap()
    }
    fn order(conn: &Connection) -> Vec<(String, Option<i64>)> {
        conn.prepare("SELECT event_uid,source_order FROM session_events WHERE source='claude' ORDER BY ts_ms,COALESCE(source_order,id),id").unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?))).unwrap().collect::<rusqlite::Result<_>>().unwrap()
    }
    fn revisions(conn: &Connection) -> Vec<i64> {
        conn.prepare("SELECT revision FROM session_events WHERE source='claude' ORDER BY event_uid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }
    #[test]
    fn complete_claude_walks_enrich_legacy_null_order_without_replacing_events() {
        for snapshot in [false, true] {
            let home = tempfile::tempdir().unwrap();
            let path = home.path().join("native-claude.jsonl");
            fs::write(&path, text(&["a", "z"])).unwrap();
            let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
            ingest_claude_transcript_as(&conn, &path, None).unwrap();
            conn.execute(
                "UPDATE session_events SET source_order=NULL WHERE source='claude'",
                [],
            )
            .unwrap();
            let before = rows(&conn);
            let native = text(&["z", "a"]);
            fs::write(&path, &native).unwrap();
            let read = || {
                if snapshot {
                    ingest_claude_transcript_text_as(&conn, &path, &native, None)
                } else {
                    ingest_claude_transcript_as(&conn, &path, None)
                }
            };
            read().unwrap();
            assert_eq!(rows(&conn), before);
            assert_eq!(
                order(&conn),
                vec![("z:0".into(), Some(0)), ("a:0".into(), Some(1))]
            );
            let after = revisions(&conn);
            read().unwrap();
            assert_eq!(revisions(&conn), after);
        }
    }
    #[test]
    fn complete_incremental_claude_walk_enriches_order_but_suffix_only_walk_does_not() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("native-claude.jsonl");
        fs::write(&path, text(&["a"])).unwrap();
        let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
        let mut cursor = transcript_cursor::TranscriptCursorState::default();
        incremental::ingest_claude_transcript_incremental(&conn, &path, None, &mut cursor).unwrap();
        conn.execute(
            "UPDATE session_events SET source_order=NULL WHERE source='claude'",
            [],
        )
        .unwrap();
        fs::write(&path, text(&["a", "z"])).unwrap();
        incremental::ingest_claude_transcript_incremental(&conn, &path, None, &mut cursor).unwrap();
        assert!(
            order(&conn).iter().all(|(_, rank)| rank.is_none()),
            "an incremental suffix cannot reconstruct unknown prefix order"
        );
        let before = rows(&conn);
        let mut full = transcript_cursor::TranscriptCursorState::default();
        incremental::ingest_claude_transcript_incremental(&conn, &path, None, &mut full).unwrap();
        assert_eq!(rows(&conn), before);
        assert_eq!(
            order(&conn),
            vec![("a:0".into(), Some(0)), ("z:0".into(), Some(1))]
        );
        let after = revisions(&conn);
        incremental::ingest_claude_transcript_incremental(&conn, &path, None, &mut full).unwrap();
        assert_eq!(revisions(&conn), after);
    }
    #[test]
    fn a_complete_claude_order_conflict_rolls_back_payload_and_cursor() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("native-claude.jsonl");
        fs::write(&path, text(&["a", "z"])).unwrap();
        let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
        ingest_claude_transcript_as(&conn, &path, None).unwrap();
        conn.execute(
            "UPDATE session_events SET location='both' WHERE source='claude'",
            [],
        )
        .unwrap();
        let before = rows(&conn);
        let rank = order(&conn);
        fs::write(&path, text(&["z", "a"])).unwrap();
        let mut cursor = transcript_cursor::TranscriptCursorState::default();
        let error =
            incremental::ingest_claude_transcript_incremental(&conn, &path, None, &mut cursor)
                .unwrap_err();
        assert!(format!("{error:#}").contains("FILE_HISTORY_SHARED_ORDER_CONFLICT"));
        assert_eq!(rows(&conn), before);
        assert_eq!(order(&conn), rank);
        assert!(cursor.file.is_none());
        assert!(cursor.claude.is_none());
    }
    #[test]
    fn complete_grok_session_reread_enriches_legacy_order_without_row_replacement() {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join(".grok/sessions/project/native-grok");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("chat_history.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"type\":\"user\",\"content\":\"first\",\"timestamp_ms\":123}\n",
                "{\"type\":\"assistant\",\"content\":\"second\",\"timestamp_ms\":123}\n"
            ),
        )
        .unwrap();
        fs::write(dir.join("summary.json"),r#"{"info":{"id":"native-grok","cwd":"/tmp/project"},"created_at":"2026-01-01T00:00:00Z"}"#).unwrap();
        let native = scan_grok_session_file(&path).unwrap().unwrap();
        let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
        ingest_grok_session(&conn, &native, &path.to_string_lossy()).unwrap();
        conn.execute(
            "UPDATE session_events SET source_order=NULL WHERE source='grok'",
            [],
        )
        .unwrap();
        let evidence = || {
            conn.prepare("SELECT id,event_uid,ts_ms,text FROM session_events WHERE source='grok' ORDER BY event_uid").unwrap().query_map([],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap()
        };
        let before = evidence();
        ingest_grok_session(&conn, &native, &path.to_string_lossy()).unwrap();
        assert_eq!(evidence(), before);
        let ranks:Vec<i64>=conn.prepare("SELECT source_order FROM session_events WHERE source='grok' ORDER BY ts_ms,source_order").unwrap().query_map([],|r|r.get(0)).unwrap().collect::<rusqlite::Result<_>>().unwrap();
        assert_eq!(ranks, vec![0, 1]);
        let versions = || {
            conn.prepare(
                "SELECT revision FROM session_events WHERE source='grok' ORDER BY event_uid",
            )
            .unwrap()
            .query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
        };
        let after = versions();
        ingest_grok_session(&conn, &native, &path.to_string_lossy()).unwrap();
        assert_eq!(versions(), after);
    }
}
