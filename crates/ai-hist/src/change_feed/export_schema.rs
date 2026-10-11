//! Reconciling the change feed with the exported shape of each kind.
//!
//! A migration can change a record's exported JSON without writing the row,
//! and so without advancing its revision. The store fingerprints each kind's
//! exported columns; an open that finds a fingerprint changed restamps the
//! rows whose exported content the change altered, above the old head, so a
//! consumer never sees an equal revision with a different payload and never
//! receives a row again for nothing.

use super::*;

pub(super) fn export_schema_digest(schema: &[(String, String)]) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(
            "relayhistory-change-feed-export-schema-v2",
            schema
        ))?)
    ))
}

pub(super) fn export_schema_digests(conn: &Connection) -> Result<BTreeMap<String, String>> {
    ChangeKind::ALL
        .iter()
        .map(|kind| {
            let schema = stored_column_schema(conn, kind.table().name)?;
            let digest = export_schema_digest(&schema)?;
            Ok((kind.as_str().to_string(), digest))
        })
        .collect()
}

pub(super) fn exported_kind_removed(
    previous: &BTreeMap<String, String>,
    current: &BTreeMap<String, String>,
) -> bool {
    previous.keys().any(|kind| !current.contains_key(kind))
}

/// The temporary table a restamp stages each row's ordinal in, keyed by the
/// fed table's rowid.
pub(super) const RESTAMP_ORDINALS: &str = "temp.change_feed_restamp_ordinals";

/// The statement that stamps every row staged in [`RESTAMP_ORDINALS`] with
/// `?1` plus its ordinal.
///
/// The ordinals come from a staged table with an INTEGER PRIMARY KEY rather
/// than a `ROW_NUMBER()` CTE in the statement itself: every fed table carries
/// change-feed triggers, and SQLite does not build an automatic index on a
/// CTE for an UPDATE of a triggered table, so the inline form rescanned the
/// whole table per row. Through the staged table each row is one primary-key
/// lookup, whatever the planner's heuristics.
pub(super) fn restamp_update_sql(table: &str) -> String {
    format!(
        "UPDATE {table}
         SET {REVISION_COLUMN} = ?1 + (
             SELECT ordinal FROM {RESTAMP_ORDINALS} o WHERE o.rowid = {table}.rowid
         )
         WHERE rowid IN (SELECT rowid FROM {RESTAMP_ORDINALS})"
    )
}

/// The rows of a kind whose exported JSON a schema change altered.
pub(super) enum RestampScope<'a> {
    /// Every row: the change can alter any row's JSON.
    Every,
    /// Rows holding a value in one of these appended columns. A row with NULL
    /// in all of them exports exactly what it did before, plus `null` for
    /// each, and an absent column already means NULL to a receiver.
    Valued(&'a [String]),
}

pub(super) fn restamp_exported_rows(
    conn: &Connection,
    kind: ChangeKind,
    scope: RestampScope,
) -> Result<()> {
    let table = kind.table().name;
    let filter = match scope {
        RestampScope::Every => String::new(),
        RestampScope::Valued(columns) => format!(
            " WHERE {}",
            columns
                .iter()
                .map(|column| format!("\"{}\" IS NOT NULL", column.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
    };
    let base: i64 = conn.query_row(
        "SELECT version FROM observation_clock WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {RESTAMP_ORDINALS};
         CREATE TABLE {RESTAMP_ORDINALS} (
             rowid INTEGER PRIMARY KEY,
             ordinal INTEGER NOT NULL
         );
         INSERT INTO {RESTAMP_ORDINALS} (rowid, ordinal)
         SELECT rowid, ROW_NUMBER() OVER (ORDER BY rowid) FROM {table}{filter};"
    ))?;
    let count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM {RESTAMP_ORDINALS}"),
        [],
        |row| row.get(0),
    )?;
    if count == 0 {
        conn.execute_batch(&format!("DROP TABLE IF EXISTS {RESTAMP_ORDINALS};"))?;
        return Ok(());
    }
    let head = base
        .checked_add(count)
        .context("change-feed revision overflow while restamping an exported schema")?;
    conn.execute(&restamp_update_sql(table), [base])?;
    // A failed restamp rolls the staged table back with the migration's
    // transaction, and the next attempt drops any leftover before staging.
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {RESTAMP_ORDINALS};"))?;
    conn.execute(
        "UPDATE observation_clock SET version=?1 WHERE singleton=1",
        [head],
    )?;
    Ok(())
}

/// The columns appended to `schema` since `previous` fingerprinted it (none
/// when it fingerprints all of `schema`), or `None` when the change is anything
/// else (a dropped, renamed, retyped or reordered column).
///
/// `ALTER TABLE ... ADD COLUMN` appends, so an additive migration leaves the
/// fingerprinted schema as a prefix of the current one.
pub(super) fn appended_columns(
    previous: &str,
    schema: &[(String, String)],
) -> Result<Option<Vec<String>>> {
    for kept in (0..=schema.len()).rev() {
        if export_schema_digest(&schema[..kept])? == previous {
            return Ok(Some(
                schema[kept..]
                    .iter()
                    .map(|(column, _)| column.clone())
                    .collect(),
            ));
        }
    }
    Ok(None)
}

/// Reconcile each kind's exported row shape. Existing feeds without a
/// fingerprint may already have delivered the older representation, so every
/// existing kind is restamped once above the current head. Later changes
/// restamp only the affected kind, and an additive change only the rows that
/// hold a value in an appended column: every other row's content is unchanged,
/// so an upgrade that adds a column never re-delivers a store's history.
/// Setting a column aside as local bookkeeping restamps nothing: a consumer
/// reads nothing from it. A newly introduced kind has no prior fingerprint and
/// is recorded without restamping: the feed-version migration that introduced
/// it already backfilled its rows above the old head.
///
/// Keeping the store epoch and cursor rows intact is deliberate. Both external
/// watermarks and named cursors can resume normally and observe the affected
/// rows at their new revisions; unrelated kinds are never replayed. Retiring a
/// kind is the one exception (see `epochs.rs`), and the surviving kinds are
/// reconciled with it, since positions that leave a kind out survive it.
pub(super) fn reconcile_export_schema(conn: &Connection, identity_existed: bool) -> Result<()> {
    let current = export_schema_digests(conn)?;
    let stored: Option<String> = conn.query_row(
        &format!("SELECT {EXPORT_SCHEMA_DIGEST_COLUMN} FROM change_feed_store WHERE singleton=1"),
        [],
        |row| row.get(0),
    )?;
    let previous = stored
        .as_deref()
        .and_then(|value| serde_json::from_str::<BTreeMap<String, String>>(value).ok());
    if previous.as_ref() == Some(&current) {
        return Ok(());
    }
    let removed = previous
        .as_ref()
        .is_some_and(|previous| exported_kind_removed(previous, &current));
    if identity_existed && removed {
        // A removed kind has no live table left to restamp and emit: a new
        // epoch makes every-kind consumers, which may hold its rows, resync,
        // while consumers that leave a kind out keep their positions -- and
        // so still need the surviving kinds restamped below.
        let removed: Vec<String> = previous
            .iter()
            .flat_map(|previous| previous.keys())
            .filter(|kind| !current.contains_key(*kind))
            .cloned()
            .collect();
        super::epochs::retire_kinds(conn, &removed)?;
    }
    if identity_existed {
        for kind in ChangeKind::ALL {
            let Some(previous) = previous.as_ref() else {
                restamp_exported_rows(conn, *kind, RestampScope::Every)?;
                continue;
            };
            let Some(digest) = previous.get(kind.as_str()) else {
                continue;
            };
            if Some(digest) == current.get(kind.as_str()) {
                continue;
            }
            let table = kind.table().name;
            // A fingerprint taken before a column became local covers the
            // whole table: its carried columns are unchanged when the
            // fingerprint is a prefix of that schema.
            let appended = match appended_columns(digest, &stored_column_schema(conn, table)?)? {
                Some(appended) => Some(appended),
                None => {
                    appended_columns(digest, &table_column_schema(conn, table)?)?.map(|appended| {
                        appended
                            .into_iter()
                            .filter(|column| !local_column(table, column))
                            .collect()
                    })
                }
            };
            match appended {
                Some(appended) if appended.is_empty() => {}
                Some(appended) => {
                    restamp_exported_rows(conn, *kind, RestampScope::Valued(&appended))?
                }
                None => restamp_exported_rows(conn, *kind, RestampScope::Every)?,
            }
        }
    }
    conn.execute(
        &format!("UPDATE change_feed_store SET {EXPORT_SCHEMA_DIGEST_COLUMN}=? WHERE singleton=1"),
        [serde_json::to_string(&current)?],
    )?;
    Ok(())
}
