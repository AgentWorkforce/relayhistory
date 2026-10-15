//! Session event insertion, including Codex layout reconciliation.
use super::*;

pub(super) fn insert_session_event(conn: &Connection, row: &EventRow<'_>) -> Result<()> {
    if row.source == "codex" {
        let native = row.identity.provider_message_id.or(Some(row.message_id));
        if let Some(native) = native.filter(|id| *id != row.event_uid) {
            let existing = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM session_events WHERE source='codex' AND session_id=?1 AND event_uid=?2 AND role=?3 AND kind=?4)",
                params![row.session_id, native, row.role, row.kind],
                |r| r.get::<_, bool>(0),
            )?;
            if existing {
                // The rollout is authoritative once it becomes complete. Keep
                // its line-derived UID so usage settlement and cursor replay
                // continue to address the row the parser actually wrote.
                conn.execute(
                    "DELETE FROM session_events WHERE source='codex' AND session_id=?1 AND event_uid=?2 AND role=?3 AND kind=?4",
                    params![row.session_id, native, row.role, row.kind],
                )?;
            }
        }
    }
    sweep_session::write_event(conn, row)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_rollout_replaces_the_same_native_sqlite_item() {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        insert_session_event(
            &conn,
            &EventRow {
                source: "codex",
                session_id: "thread",
                message_id: "msg-1",
                event_uid: "msg-1",
                ts_ms: 1,
                role: "assistant",
                kind: "text",
                text: Some("SQLite copy"),
                ..Default::default()
            },
        )
        .unwrap();
        insert_session_event(
            &conn,
            &EventRow {
                source: "codex",
                session_id: "thread",
                message_id: "msg-1",
                identity: RequestIdentity {
                    provider_message_id: Some("msg-1"),
                    ..Default::default()
                },
                event_uid: "2:agent_message",
                ts_ms: 2,
                role: "assistant",
                kind: "text",
                text: Some("Rollout copy"),
                ..Default::default()
            },
        )
        .unwrap();
        let rows: Vec<(String, String)> = conn
            .prepare("SELECT event_uid,text FROM session_events WHERE source='codex' AND session_id='thread'")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(rows, [("2:agent_message".into(), "Rollout copy".into())]);
    }
}
