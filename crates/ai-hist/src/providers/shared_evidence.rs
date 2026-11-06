//! Protect remote-backed provider evidence from native rewrites. The actual
//! writers run in the caller's transaction; verify their effective persisted
//! payloads before commit, rolling back the whole capture on a conflict.
use crate::EvidenceKind;
use anyhow::{Context, Result};
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(crate) struct Snapshot {
    source: &'static str,
    rows: BTreeMap<(&'static str, String), [u8; 32]>,
    /// A null legacy rank asserts no ordering. Only known source ranks are
    /// protected, independently of the exact non-order payload fingerprint.
    orders: BTreeMap<String, i64>,
}
impl Snapshot {
    pub(crate) fn capture(conn: &Connection, source: &'static str, sid: &str) -> Result<Self> {
        anyhow::ensure!(
            !conn.is_autocommit(),
            "{}_SHARED_GUARD_TRANSACTION_REQUIRED",
            source.to_ascii_uppercase()
        );
        let mut rows = BTreeMap::new();
        let mut orders = BTreeMap::new();
        for (kind, table, key) in [
            (EvidenceKind::SessionEvent, "session_events", "event_uid"),
            (EvidenceKind::SessionMarker, "session_markers", "marker_uid"),
            (EvidenceKind::ToolCall, "tool_calls", "tool_use_id"),
        ] {
            let columns = kind.owned_columns().map(str::to_owned).collect::<Vec<_>>();
            let mut statement = conn.prepare(&format!(
                "SELECT {} FROM {table} WHERE source=?1 AND session_id=?2 AND location IS NOT 'local'", columns.join(",")
            ))?;
            let key_index = columns
                .iter()
                .position(|name| name == key)
                .context("provider evidence key missing")?;
            let mut selected = statement.query([source, sid])?;
            while let Some(row) = selected.next()? {
                let record_key: String = row.get(key_index)?;
                if kind == EvidenceKind::SessionEvent {
                    if let Some(order) = row.get::<_, Option<i64>>("source_order")? {
                        orders.insert(record_key.clone(), order);
                    }
                }
                rows.insert((table, record_key), fingerprint(row, &columns)?);
            }
        }
        // History has no location column. Match its canonical key to records
        // that remote intake actually owns; merely having remote observation
        // data for the session must not freeze local-only prompts.
        let columns = EvidenceKind::History
            .owned_columns()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let selected_columns = columns
            .iter()
            .map(|name| format!("h.{name} AS {name}"))
            .collect::<Vec<_>>()
            .join(",");
        let mut statement = conn.prepare(&format!(
            "SELECT {selected_columns} FROM observation_evidence oe CROSS JOIN history h \
             WHERE oe.source=?1 AND oe.session_id=?2 AND oe.location='remote' \
             AND h.source=oe.source AND h.session_id=oe.session_id \
             AND ((h.event_uid IS NOT NULL AND h.event_uid=json_extract(oe.payload_json,'$.item.record.payload.event_uid')) OR (h.event_uid IS NULL AND json_extract(oe.payload_json,'$.item.record.payload.event_uid') IS NULL AND h.timestamp_ms=json_extract(oe.payload_json,'$.item.record.payload.timestamp_ms') AND h.prompt=json_extract(oe.payload_json,'$.item.record.payload.prompt'))) \
             AND json_extract(oe.payload_json,'$.item.managed')=1 \
             AND json_extract(oe.payload_json,'$.item.record.kind')='history'"
        ))?;
        let mut selected = statement.query([source, sid])?;
        while let Some(row) = selected.next()? {
            let timestamp_ms: i64 = row.get("timestamp_ms")?;
            let prompt: String = row.get("prompt")?;
            let event_uid: Option<String> = row.get("event_uid")?;
            let key = serde_json::to_string(&(timestamp_ms, prompt, event_uid))?;
            rows.insert(("history", key), fingerprint(row, &columns)?);
        }
        Ok(Self {
            source,
            rows,
            orders,
        })
    }
    pub(crate) fn ensure_unchanged(&self, conn: &Connection, sid: &str) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let current = Self::capture(conn, self.source, sid)?;
        for (key, digest) in &self.rows {
            anyhow::ensure!(current.rows.get(key) == Some(digest),
                "{}_SHARED_EVIDENCE_CONFLICT: native rewrite changes remote-backed {} {}; prior snapshot retained, retry after ownership is reconciled", self.source.to_ascii_uppercase(), key.0, key.1);
        }
        for (uid, order) in &self.orders {
            anyhow::ensure!(current.orders.get(uid) == Some(order),
                "{}_SHARED_EVIDENCE_CONFLICT: native rewrite changes remote-backed session_events order {}; prior snapshot retained, retry after ownership is reconciled", self.source.to_ascii_uppercase(), uid);
        }
        Ok(())
    }
}
fn fingerprint(row: &rusqlite::Row<'_>, columns: &[String]) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    for (index, name) in columns.iter().enumerate() {
        // Ordering is checked separately against the previous known rank.
        // Omitting this optional fact did not assert a null native position.
        if name == "source_order" {
            continue;
        }
        bytes(&mut digest, name.as_bytes());
        match row.get_ref(index)? {
            ValueRef::Null => digest.update(b"null"),
            ValueRef::Integer(value) => {
                digest.update(b"integer");
                digest.update(value.to_le_bytes());
            }
            ValueRef::Real(value) => {
                digest.update(b"real");
                digest.update(value.to_bits().to_le_bytes());
            }
            ValueRef::Text(value) => {
                digest.update(b"text");
                bytes(&mut digest, value);
            }
            ValueRef::Blob(value) => {
                digest.update(b"blob");
                bytes(&mut digest, value);
            }
        }
    }
    Ok(digest.finalize().into())
}
fn bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_change(initial_order: Option<i64>, mutation: &str) -> Result<()> {
        let home = tempfile::tempdir()?;
        let conn = crate::store::open_db(&home.path().join("history.db"))?;
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text,location,source_order) VALUES('hermes','legacy','native-event',100,'assistant','text','original','remote',?1)",
            [initial_order],
        )?;
        let snapshot = Snapshot::capture(&tx, "hermes", "legacy")?;
        tx.execute_batch(mutation)?;
        snapshot.ensure_unchanged(&tx, "legacy")
    }

    #[test]
    fn unknown_source_order_accepts_native_enrichment_but_payload_changes_stay_protected() {
        check_change(None, "UPDATE session_events SET source_order=0").unwrap();
        let error = check_change(
            None,
            "UPDATE session_events SET source_order=0,text='changed'",
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("HERMES_SHARED_EVIDENCE_CONFLICT"));
    }

    #[test]
    fn known_source_order_cannot_change_or_be_cleared() {
        check_change(Some(0), "UPDATE session_events SET source_order=0").unwrap();
        for mutation in [
            "UPDATE session_events SET source_order=1",
            "UPDATE session_events SET source_order=NULL",
        ] {
            let error = check_change(Some(0), mutation).unwrap_err();
            assert!(error
                .to_string()
                .contains("HERMES_SHARED_EVIDENCE_CONFLICT"));
        }
    }
}
