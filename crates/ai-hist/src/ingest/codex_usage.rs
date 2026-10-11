//! Codex usage is attached after the assistant row that earned it.
//!
//! A re-read inserts that row before the `token_count` arrives, so the
//! conflict keeps a stored `token_json` instead of nulling it. This pass
//! then writes the measurement only when it differs, and clears a stored
//! measurement only once it knows the row ends without one.

use anyhow::Result;
use rusqlite::{params, Connection};
use std::collections::HashSet;

use super::{surviving_refusals, PendingCodexUsage, UnreadableSnapshot};

/// Assistant rows this pass wrote before their usage was known.
#[derive(Default)]
pub(super) struct Pass {
    unsettled: HashSet<String>,
}

impl Pass {
    /// `settled` means the row already carries this pass's usage.
    /// Otherwise it stays unsettled until [`Self::attach`], and a pass that
    /// never measures it clears a stale fact once.
    pub(super) fn wrote(&mut self, settled: bool, uid: &str) -> Option<String> {
        if settled {
            self.unsettled.remove(uid);
            None
        } else {
            self.unsettled.insert(uid.to_string());
            Some(uid.to_string())
        }
    }

    /// Attach measured usage to the assistant event that earned it, or hold
    /// it until one appears.
    ///
    /// The statement matches a row only when the stored JSON differs. An
    /// equal value writes nothing, so a re-read does not stamp a revision.
    pub(super) fn attach(
        &mut self,
        conn: &Connection,
        session_id: &str,
        pending: &mut Option<PendingCodexUsage>,
        untokened: &mut Option<String>,
        usage: PendingCodexUsage,
    ) -> Result<()> {
        match untokened.take() {
            Some(uid) => {
                self.unsettled.remove(&uid);
                let token_json = usage.into_token_json();
                conn.execute(
                    "UPDATE session_events SET token_json = ?1 \
                     WHERE source = 'codex' AND session_id = ?2 AND event_uid = ?3 \
                       AND token_json IS NOT ?1",
                    params![token_json, session_id, uid],
                )?;
            }
            None => *pending = Some(usage),
        }
        Ok(())
    }

    fn clear_unsettled(
        &self,
        conn: &Connection,
        session_id: &str,
        refused: &HashSet<String>,
    ) -> Result<()> {
        let clearing: Vec<&str> = self
            .unsettled
            .iter()
            .filter(|uid| !refused.contains(uid.as_str()))
            .map(String::as_str)
            .collect();
        if clearing.is_empty() {
            return Ok(());
        }
        // Remote-only rows keep the usage they already had. A row this pass
        // just handed back to remote is already `remote` and stays out too.
        let noted = serde_json::to_string(&clearing).unwrap_or_else(|_| "[]".to_string());
        conn.execute(
            "UPDATE session_events SET token_json = NULL \
             WHERE source = 'codex' AND session_id = ?1 \
               AND location <> 'remote' \
               AND token_json IS NOT NULL \
               AND event_uid IN (SELECT value FROM json_each(?2))",
            params![session_id, noted],
        )?;
        Ok(())
    }
}

/// Clear usage this pass did not re-measure, then fill a refusal into a row
/// that still has none.
///
/// A surviving refusal is the usage that row ends with. It is not cleared,
/// and it is written only where `token_json` is null, so a measurement the
/// turn acquired by another route stays.
pub(super) fn finish(
    conn: &Connection,
    session_id: &str,
    pass: &Pass,
    unreadable: &[UnreadableSnapshot],
    measured: &HashSet<u64>,
) -> Result<()> {
    let refusals = surviving_refusals(unreadable, measured);
    let refused: HashSet<String> = refusals.iter().map(|(uid, _)| uid.clone()).collect();
    pass.clear_unsettled(conn, session_id, &refused)?;
    for (uid, raw) in refusals {
        conn.execute(
            "UPDATE session_events SET token_json = ?1 \
             WHERE source = 'codex' AND session_id = ?2 AND event_uid = ?3 \
               AND token_json IS NULL",
            params![
                PendingCodexUsage::Unusable(raw).into_token_json(),
                session_id,
                uid
            ],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{CodexTokenTotals, PendingCodexUsage, UnreadableSnapshot};
    use super::*;
    use rusqlite::Connection;

    fn db() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&dir.path().join("ai-history.db")).unwrap();
        (dir, conn)
    }

    fn totals(output: u64) -> CodexTokenTotals {
        CodexTokenTotals {
            input: 5,
            cached_input: 1,
            cache_write: 0,
            output,
            reasoning_output: 0,
            total: 5 + output,
        }
    }

    fn usage(output: u64) -> PendingCodexUsage {
        PendingCodexUsage::Delta(totals(output))
    }

    fn json(output: u64) -> String {
        totals(output).to_token_json()
    }

    fn insert(conn: &Connection, uid: &str, token_json: Option<&str>, location: &str) {
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, event_uid, token_json, location) \
             VALUES ('codex', 's', 1, 'assistant', 'tool_use', ?1, ?2, ?3)",
            params![uid, token_json, location],
        )
        .unwrap();
    }

    fn token(conn: &Connection, uid: &str) -> Option<String> {
        conn.query_row(
            "SELECT token_json FROM session_events WHERE event_uid = ?1",
            [uid],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn revision(conn: &Connection, uid: &str) -> i64 {
        conn.query_row(
            "SELECT revision FROM session_events WHERE event_uid = ?1",
            [uid],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn codex_reread_attach_writes_only_when_usage_differs() {
        let (_dir, conn) = db();
        insert(&conn, "e1", Some(&json(1)), "local");
        let before = revision(&conn, "e1");
        let mut pass = Pass::default();
        let mut pending = None;
        let mut untokened = Some("e1".to_string());
        pass.attach(&conn, "s", &mut pending, &mut untokened, usage(1))
            .unwrap();
        assert_eq!(
            revision(&conn, "e1"),
            before,
            "the same usage writes nothing"
        );
        assert_eq!(token(&conn, "e1").as_deref(), Some(json(1).as_str()));

        let mut untokened = Some("e1".to_string());
        pass.attach(&conn, "s", &mut pending, &mut untokened, usage(2))
            .unwrap();
        assert!(revision(&conn, "e1") > before);
        let changed = revision(&conn, "e1");
        assert_eq!(token(&conn, "e1").as_deref(), Some(json(2).as_str()));

        let mut untokened = Some("e1".to_string());
        pass.attach(&conn, "s", &mut pending, &mut untokened, usage(2))
            .unwrap();
        assert_eq!(
            revision(&conn, "e1"),
            changed,
            "writing it again is not a second change"
        );
    }

    #[test]
    fn codex_reread_clear_drops_local_usage_once_and_keeps_remote() {
        let (_dir, conn) = db();
        insert(&conn, "local-1", Some(&json(1)), "local");
        insert(&conn, "remote-1", Some(&json(2)), "remote");
        insert(&conn, "both-1", Some(&json(3)), "both");
        let remote_rev = revision(&conn, "remote-1");
        let mut pass = Pass::default();
        pass.wrote(false, "local-1");
        pass.wrote(false, "remote-1");
        pass.wrote(false, "both-1");
        pass.clear_unsettled(&conn, "s", &HashSet::new()).unwrap();
        assert_eq!(token(&conn, "local-1"), None);
        assert_eq!(token(&conn, "both-1"), None);
        assert_eq!(token(&conn, "remote-1").as_deref(), Some(json(2).as_str()));
        assert_eq!(revision(&conn, "remote-1"), remote_rev);
        let local_rev = revision(&conn, "local-1");
        pass.clear_unsettled(&conn, "s", &HashSet::new()).unwrap();
        assert_eq!(
            revision(&conn, "local-1"),
            local_rev,
            "a second clear writes nothing"
        );
        assert_eq!(revision(&conn, "remote-1"), remote_rev);
    }

    #[test]
    fn codex_reread_refusal_fills_only_a_null_row() {
        let (_dir, conn) = db();
        insert(&conn, "measured", Some(&json(4)), "local");
        insert(&conn, "empty", None, "local");
        let measured_rev = revision(&conn, "measured");
        let pass = Pass::default();
        let unreadable = vec![
            UnreadableSnapshot {
                uid: "measured".to_string(),
                generation: 0,
                raw: "{\"input_tokens\":-1}".to_string(),
            },
            UnreadableSnapshot {
                uid: "empty".to_string(),
                generation: 0,
                raw: "{\"input_tokens\":-1}".to_string(),
            },
        ];
        finish(&conn, "s", &pass, &unreadable, &HashSet::new()).unwrap();
        assert_eq!(token(&conn, "measured").as_deref(), Some(json(4).as_str()));
        assert_eq!(revision(&conn, "measured"), measured_rev);
        assert_eq!(
            token(&conn, "empty").as_deref(),
            Some("{\"input_tokens\":-1}")
        );
        let filled = revision(&conn, "empty");
        finish(&conn, "s", &pass, &unreadable, &HashSet::new()).unwrap();
        assert_eq!(
            revision(&conn, "empty"),
            filled,
            "the refusal is not written twice"
        );
        assert_eq!(revision(&conn, "measured"), measured_rev);
    }
}
