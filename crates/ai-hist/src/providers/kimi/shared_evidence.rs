//! Protect remote-backed evidence from positional native rewrites. The actual
//! writers run in the caller's transaction; verify their effective persisted
//! payloads before commit, rolling back the whole capture on a conflict.
use super::*;
use rusqlite::types::ValueRef;
use std::collections::BTreeMap;

pub(super) struct Snapshot {
    rows: BTreeMap<(&'static str, String), [u8; 32]>,
}
impl Snapshot {
    pub(super) fn capture(conn: &Connection, sid: &str) -> Result<Self> {
        anyhow::ensure!(
            !conn.is_autocommit(),
            "KIMI_SHARED_GUARD_TRANSACTION_REQUIRED"
        );
        let mut rows = BTreeMap::new();
        for (table, key) in [
            ("session_events", "event_uid"),
            ("session_markers", "marker_uid"),
            ("tool_calls", "tool_use_id"),
        ] {
            let mut statement = conn.prepare(&format!(
                "SELECT * FROM {table} WHERE source=?1 AND session_id=?2 AND location IS NOT 'local'"
            ))?;
            let columns = statement
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let key_index = columns
                .iter()
                .position(|name| name == key)
                .context("Kimi evidence key missing")?;
            let mut selected = statement.query([ID, sid])?;
            while let Some(row) = selected.next()? {
                rows.insert((table, row.get(key_index)?), fingerprint(row, &columns)?);
            }
        }
        Ok(Self { rows })
    }
    pub(super) fn ensure_unchanged(&self, conn: &Connection, sid: &str) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let current = Self::capture(conn, sid)?;
        for (key, digest) in &self.rows {
            anyhow::ensure!(current.rows.get(key) == Some(digest),
                "KIMI_SHARED_EVIDENCE_CONFLICT: native rewrite changes remote-backed {} {}; prior snapshot retained, retry after ownership is reconciled", key.0, key.1);
        }
        Ok(())
    }
}
fn fingerprint(row: &rusqlite::Row<'_>, columns: &[String]) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    for (index, name) in columns.iter().enumerate() {
        if matches!(name.as_str(), "id" | "location" | "revision") {
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
