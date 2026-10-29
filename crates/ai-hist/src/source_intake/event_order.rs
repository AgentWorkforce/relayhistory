//! Snapshot ordering is independent of event identity and snapshot array order.
use super::*;

/// Validate all retained observations together before any canonical mutation.
/// Unknown legacy order never overrides known order; two known assertions must
/// agree, including rank uniqueness among separately contributed events.
pub(super) fn validate_union(
    conn: &Connection,
    key: &ObservationKey,
    snapshots: &BTreeMap<(u8, String, String), RecordSnapshot>,
    managed: &BTreeSet<String>,
) -> Result<()> {
    let mut ranks = BTreeMap::new();
    let mut positions = BTreeMap::new();
    for snapshot in snapshots.values() {
        for item in &snapshot.records {
            let record = &item.record;
            if record.kind != EvidenceKind::SessionEvent {
                continue;
            }
            let Some(rank) = record.payload.get("source_order").and_then(Value::as_i64) else {
                continue;
            };
            let ts = record.payload["ts_ms"]
                .as_i64()
                .context("validated event timestamp")?;
            let uid = record.payload["event_uid"]
                .as_str()
                .context("validated event identity")?;
            if let Some(previous) = ranks.insert((ts, uid), rank) {
                ensure!(
                    previous == rank,
                    "SOURCE_ORDER_CONFLICT: retained observations disagree on event order"
                );
            }
            if let Some(previous) = positions.insert((ts, rank), uid) {
                ensure!(
                    previous == uid,
                    "SOURCE_ORDER_CONFLICT: distinct events claim the same source order"
                );
            }
            if !managed.contains(&record.identity()) {
                validate_protected(conn, key, record, rank)?;
            }
        }
    }
    Ok(())
}

fn validate_protected(
    conn: &Connection,
    key: &ObservationKey,
    record: &EvidenceRecord,
    rank: i64,
) -> Result<()> {
    use rusqlite::OptionalExtension;
    let existing: Option<Option<i64>> = conn.query_row(
        "SELECT source_order FROM session_events WHERE source=?1 AND session_id=?2 AND event_uid=?3",
        rusqlite::params![key.source,key.session_id,record.payload["event_uid"].as_str()],
        |row| row.get(0),
    ).optional()?;
    if let Some(existing) = existing {
        ensure!(existing == Some(rank), "SOURCE_ORDER_CONFLICT: protected canonical evidence has different or unknown source order");
    }
    Ok(())
}

pub(super) fn merge_record(
    union: &mut BTreeMap<String, EvidenceRecord>,
    identity: String,
    record: &EvidenceRecord,
) {
    let winner = union.entry(identity).or_insert_with(|| record.clone());
    if winner.kind == EvidenceKind::SessionEvent
        && winner
            .payload
            .get("source_order")
            .is_none_or(Value::is_null)
        && record
            .payload
            .get("source_order")
            .is_some_and(|value| !value.is_null())
    {
        winner.payload.insert(
            "source_order".into(),
            record.payload["source_order"].clone(),
        );
    }
}

pub(super) fn apply_union(
    conn: &Connection,
    key: &ObservationKey,
    snapshots: &BTreeMap<(u8, String, String), RecordSnapshot>,
    previously_managed: &BTreeMap<String, EvidenceRecord>,
) -> Result<()> {
    let mut union = BTreeMap::new();
    let mut managed = BTreeSet::new();
    for snapshot in snapshots.values() {
        for item in &snapshot.records {
            let identity = item.record.identity();
            if item.managed {
                managed.insert(identity.clone());
                merge_record(&mut union, identity, &item.record);
            }
        }
    }
    validate_union(conn, key, snapshots, &managed)?;
    for (identity, record) in previously_managed {
        if !union.contains_key(identity) {
            record.remove(conn, key.location)?;
        }
    }
    retirement::retain_support(conn, key, union.keys())?;
    for record in union.into_values() {
        record.write(conn, key.location)?;
    }
    Ok(())
}
