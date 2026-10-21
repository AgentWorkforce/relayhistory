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
}
impl Snapshot {
    pub(crate) fn capture(conn: &Connection, source: &'static str, sid: &str) -> Result<Self> {
        anyhow::ensure!(
            !conn.is_autocommit(),
            "{}_SHARED_GUARD_TRANSACTION_REQUIRED",
            source.to_ascii_uppercase()
        );
        let mut rows = BTreeMap::new();
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
                rows.insert((table, row.get(key_index)?), fingerprint(row, &columns)?);
            }
        }
        Ok(Self { source, rows })
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
        Ok(())
    }
}
fn fingerprint(row: &rusqlite::Row<'_>, columns: &[String]) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    for (index, name) in columns.iter().enumerate() {
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
