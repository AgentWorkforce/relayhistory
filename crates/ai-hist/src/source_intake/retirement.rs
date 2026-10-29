//! Proof of remote support captured before a direct local source retires.
//! This is local ownership state, never a rewrite of acquired snapshots or
//! their optimistic revision fences.
use super::*;

pub(crate) struct Retained(Vec<EvidenceRecord>);

pub(crate) fn capture(conn: &Connection, source: &str, sid: &str) -> Result<Retained> {
    let mut retained = BTreeMap::new();
    let mut conflicts = BTreeMap::new();
    for observation in observations::list(conn, source, sid)? {
        if observation.key.location != SessionLocation::Remote {
            continue;
        }
        for item in read_snapshot(conn, &observation.key)?.records {
            if !retired_kind(item.record.kind) {
                continue;
            }
            let identity = item.record.identity();
            if item.record.matches_canonical(conn)? {
                retained.entry(identity).or_insert(item.record);
            } else {
                // A conflicting retained snapshot is not authority to replace
                // the retiring source's data now or on a later replay.
                observations::protect_canonical_evidence(conn, &observation.key, &identity)?;
                conflicts.insert(identity, item.record);
            }
        }
    }
    for (identity, record) in conflicts {
        if !retained.contains_key(&identity) {
            record.revoke_unmatched_remote_share(conn)?;
        }
    }
    Ok(Retained(retained.into_values().collect()))
}

fn retired_kind(kind: EvidenceKind) -> bool {
    matches!(kind, EvidenceKind::History | EvidenceKind::SessionEvent |
        EvidenceKind::ToolCall | EvidenceKind::FileEdit | EvidenceKind::SessionMarker)
}

impl Retained {
    pub(crate) fn restore(self, conn: &Connection, source: &str, sid: &str) -> Result<()> {
        let restored = !self.0.is_empty();
        for record in self.0 {
            // Retirement may retain a row another side backs. Never overwrite
            // an intervening conflicting row, even within the caller's transaction.
            if record.exists(conn)? && !record.matches_canonical(conn)? {
                continue;
            }
            record.write(conn, SessionLocation::Remote)?;
            let identity = record.identity();
            conn.execute("INSERT OR IGNORE INTO local_retirement_support(source,session_id,record_identity) VALUES(?1,?2,?3)", rusqlite::params![source,sid,identity])?;
            conn.execute("DELETE FROM canonical_evidence_protection WHERE source=?1 AND session_id=?2 AND record_identity=?3", rusqlite::params![source,sid,identity])?;
        }
        if restored {
            crate::store::refresh_project_identity(conn)?;
        }
        Ok(())
    }
}

pub(super) fn restore_management(
    conn: &Connection,
    key: &ObservationKey,
    mut snapshot: RecordSnapshot,
) -> Result<RecordSnapshot> {
    let supported = conn.prepare("SELECT record_identity FROM local_retirement_support WHERE source=?1 AND session_id=?2")?
        .query_map(rusqlite::params![key.source,key.session_id], |row| row.get::<_,String>(0))?
        .collect::<rusqlite::Result<BTreeSet<_>>>()?;
    for item in &mut snapshot.records {
        if supported.contains(&item.record.identity()) && item.record.matches_canonical(conn)? {
            item.managed = true;
        }
    }
    Ok(snapshot)
}

// A managed winner must be compared before an unmanaged conflicting sibling;
// the latter is retained evidence, not authority over the canonical payload.
pub(super) fn previous_winners<'a>(
    conn: &Connection,
    snapshots: &'a BTreeMap<(u8, String, String), RecordSnapshot>,
) -> Result<BTreeMap<String, &'a EvidenceRecord>> {
    let mut previous_winners = BTreeMap::new();
    for snapshot in snapshots.values() {
        for item in &snapshot.records {
            let entry = previous_winners.entry(item.record.identity());
            if item.managed && item.record.matches_canonical(conn)? {
                entry.and_modify(|record| *record = &item.record).or_insert(&item.record);
            } else {
                entry.or_insert(&item.record);
            }
        }
    }
    Ok(previous_winners)
}

// Do not let an old ownership proof authorize an unrelated later row that
// reuses an identity after every supporting connector withdrew it.
pub(super) fn retain_support<'a>(
    conn: &Connection,
    key: &ObservationKey,
    retained: impl Iterator<Item = &'a String>,
) -> Result<()> {
    let identities = serde_json::to_string(&retained.collect::<Vec<_>>())?;
    conn.execute("DELETE FROM local_retirement_support WHERE source=?1 AND session_id=?2 AND record_identity NOT IN (SELECT value FROM json_each(?3))", rusqlite::params![key.source,key.session_id,identities])?;
    Ok(())
}

// Retirement proved a particular payload, not permission for a conflicting
// sibling to inherit another connector's ownership of the same identity.
pub(super) fn may_inherit(
    conn: &Connection,
    record: &EvidenceRecord,
    prior: &[ManagedRecord],
) -> Result<bool> {
    let identity = record.identity();
    let supported: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM local_retirement_support WHERE record_identity=?1 AND source=?2 AND session_id=?3)",
        rusqlite::params![identity,record.payload.get("source").and_then(Value::as_str),record.payload.get("session_id").and_then(Value::as_str)],
        |row| row.get(0),
    )?;
    Ok(!supported || prior.iter().any(|item| item.managed && item.record.identity() == identity)
        || record.matches_canonical(conn)?)
}

pub(super) fn management_allowed(
    conn: &Connection,
    record: &EvidenceRecord,
    prior: &[ManagedRecord],
    revoked: &BTreeSet<String>,
    previously_managed: &BTreeMap<String, EvidenceRecord>,
) -> Result<bool> {
    Ok(!revoked.contains(&record.identity())
        && may_inherit(conn, record, prior)?
        && (previously_managed.contains_key(&record.identity()) || !record.exists(conn)?))
}
