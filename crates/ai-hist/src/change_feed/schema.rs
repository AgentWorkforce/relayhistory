//! The feed's schema: the revision columns, tombstone and cursor tables,
//! and the triggers that stamp every write.

use super::{
    feed_identity_exists, reconcile_export_schema, stored_columns, ChangeKind,
    EXPORT_SCHEMA_DIGEST_COLUMN, KINDS_COLUMN, MIGRATION, REVISION_COLUMN,
};
use crate::store::{ensure_columns, migration_applied};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};

/// The catalog row a consumer receives carries `locations`, which is derived
/// from `session_presences` rather than stored on `sessions`. A presence
/// coming or going is therefore a change to the session row as the feed
/// reports it, and must re-stamp that row even though `sessions` itself was
/// not written — a subagent cleanup that drops the local presence and keeps
/// the remote one changes nothing else.
pub(super) const PRESENCE_TRIGGERS: &[&str] = &[
    "change_feed_session_locations_insert",
    "change_feed_session_locations_update",
    "change_feed_session_locations_delete",
];

/// The names the session re-stamp triggers had before a presence was a kind
/// of its own. They are the presence kind's stamping triggers' names now, so
/// the migration to [`MIGRATION`] drops them before either set is created.
const RETIRED_PRESENCE_TRIGGERS: &[&str] = &[
    "change_feed_session_presences_insert",
    "change_feed_session_presences_update",
    "change_feed_session_presences_delete",
];

pub(super) fn trigger_names(kind: ChangeKind) -> [String; 3] {
    let name = kind.table().name;
    [
        format!("change_feed_{name}_insert"),
        format!("change_feed_{name}_update"),
        format!("change_feed_{name}_delete"),
    ]
}

/// The columns of `table` an update must change to take a new revision:
/// every column [`stored_columns`] carries, which is every column but
/// `revision` itself, read from the live schema.
///
/// Read from the live schema rather than a list in code because columns are
/// added over time (`ensure_columns`, the `REQUIRED_*_COLUMNS` lists): a
/// column a migration adds is guarded as soon as [`init_schema`] regenerates
/// the trigger, which [`schema_is_current`] makes it do on the open after the
/// column appears.
///
/// No column is left out, so a consumer that replays the feed holds every
/// table exactly, column for column. Each column was considered:
///
/// - The two that are only "when this database last wrote the row" --
///   `session_relationships.updated_ms` and `session_observations.updated_ms`
///   -- were the ones a re-read rewrote with `now` and nothing else. Leaving
///   them out here would have made a consumer's copy drift from the table;
///   instead their writers (`record_relationship`, `observations::upsert`)
///   skip an upsert that changes nothing else, so the stamp is the time of the
///   last change and an unchanged re-read writes nothing.
/// - `session_relationships.created_ms` and
///   `session_commit_links.created_at_ms` are written once; no upsert
///   rewrites them.
/// - `sessions.source_stamp` / `discovery_state` / `parser_version` and the
///   presence and observation stamps move only when the source moved or the
///   parser changed, and the catalog digest behind `SyncReport::changed`
///   relies on every write to one of them moving the head.
pub(crate) fn stamped_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    stored_columns(conn, table)
}

/// The `WHEN` clause of `table`'s update trigger.
///
/// `NEW.revision = OLD.revision` is what stops the trigger re-firing on its
/// own stamp under `recursive_triggers` (the stamp changes `revision` and
/// only that). The rest is what makes an upsert that rewrites a row with the
/// values it already holds leave it alone: `IS NOT` rather than `<>`, so a
/// NULL becoming a value, or a value becoming NULL, is a change. A change of
/// identity changes a key column, so it is always stamped.
///
/// Generated from the live column list; [`update_trigger_is_current`]
/// compares an installed trigger against it, so the text here is also the
/// fingerprint that decides when a trigger is rebuilt.
fn update_guard(conn: &Connection, table: &str) -> Result<String> {
    let columns = stamped_columns(conn, table)?;
    anyhow::ensure!(
        !columns.is_empty(),
        "change feed: {table} has no columns to guard its update trigger with"
    );
    let changed = columns
        .iter()
        .map(|column| format!("NEW.{column} IS NOT OLD.{column}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    Ok(format!(
        "WHEN NEW.{REVISION_COLUMN} = OLD.{REVISION_COLUMN} AND ({changed})"
    ))
}

/// The guard of the presence trigger that re-stamps a session when one of
/// its presences moves. The catalog row derives `locations` from which
/// presences *exist*, so an update that keeps a presence's identity -- a new
/// `source_stamp` or `raw_locator` on every append -- cannot change the row
/// the feed reports for the session, and re-stamping it would report it
/// unchanged. The presence row itself is stamped by its own kind's trigger.
const PRESENCE_MOVED_GUARD: &str = "WHEN NEW.revision = OLD.revision AND \
     (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id \
      OR OLD.location IS NOT NEW.location)";

/// The stored text of trigger `name`, or `None` when it does not exist.
pub(super) fn trigger_sql(conn: &Connection, name: &str) -> Result<Option<String>> {
    Ok(conn
        .prepare_cached("SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?")?
        .query_row([name], |row| row.get::<_, Option<String>>(0))
        .optional()?
        .flatten())
}

/// Whether trigger `name` exists and carries `guard`. SQLite keeps a
/// trigger's text as written past its first keywords, so the guard is found
/// verbatim in a trigger built from it, and a trigger built before a column
/// was added -- or before the guard existed at all -- does not contain it.
fn update_trigger_is_current(conn: &Connection, name: &str, guard: &str) -> Result<bool> {
    Ok(trigger_sql(conn, name)?.is_some_and(|sql| sql.contains(guard)))
}

/// Drop `table`'s change-feed update trigger, ahead of an `ALTER TABLE ...
/// DROP COLUMN` on it.
///
/// The trigger's guard names every column of its table, and SQLite refuses to
/// drop a column a trigger names. The migration pass that drops the column
/// recreates the trigger from the remaining columns in [`init_schema`], which
/// runs last in the same pass; until then, updates to `table` take no
/// revision, so nothing between the two may update it. A table the feed does
/// not stamp is left alone.
pub(crate) fn release_update_guard(conn: &Connection, table: &str) -> Result<()> {
    if let Some(kind) = ChangeKind::ALL
        .iter()
        .find(|kind| kind.table().name == table)
    {
        let [_, update, _] = trigger_names(*kind);
        conn.execute_batch(&format!("DROP TRIGGER IF EXISTS {update};"))?;
    }
    Ok(())
}

/// Whether every write that changes the catalog (`sessions`) -- an insert, a
/// delete, an update that changes a column -- moves the database-wide
/// clock: the clock and the feed's identity exist, and the three `sessions`
/// triggers that bump it are installed. This is the one guarantee a catalog
/// digest keyed on the head needs, checked in five schema lookups rather than
/// the full [`schema_is_current`] walk over every feed table, since it runs
/// on every tick, forced or not.
pub(crate) fn catalog_writes_move_the_head(conn: &Connection) -> Result<bool> {
    let mut object = conn.prepare_cached("SELECT 1 FROM sqlite_master WHERE name = ? LIMIT 1")?;
    for name in ["observation_clock", "change_feed_store"] {
        if !object.exists([name])? {
            return Ok(false);
        }
    }
    for trigger in trigger_names(ChangeKind::Session) {
        if !object.exists([trigger])? {
            return Ok(false);
        }
    }
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM change_feed_store WHERE singleton = 1)",
        [],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

/// Whether every one of `names` is in `sqlite_master`, through `object`
/// (`SELECT 1 FROM sqlite_master WHERE name = ?`).
fn all_exist(
    object: &mut rusqlite::Statement<'_>,
    names: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<bool> {
    for name in names {
        if !object.exists([name.as_ref()])? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether this database has everything [`init_schema`] would add.
pub(crate) fn schema_is_current(conn: &Connection) -> Result<bool> {
    let mut object = conn.prepare("SELECT 1 FROM sqlite_master WHERE name = ? LIMIT 1")?;
    if !feed_store_is_current(conn, &mut object)? {
        return Ok(false);
    }
    for kind in ChangeKind::ALL {
        if !kind_feed_is_current(conn, &mut object, *kind)? {
            return Ok(false);
        }
    }
    if !all_exist(&mut object, PRESENCE_TRIGGERS)? {
        return Ok(false);
    }
    if !update_trigger_is_current(conn, PRESENCE_TRIGGERS[1], PRESENCE_MOVED_GUARD)? {
        return Ok(false);
    }
    let bound: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('consumer_cursors') WHERE name = ?)",
        [KINDS_COLUMN],
        |row| row.get(0),
    )?;
    if !bound {
        return Ok(false);
    }
    migration_applied(conn, MIGRATION)
}

/// Whether the feed's own tables exist and its identity row carries an
/// export-schema digest.
fn feed_store_is_current(conn: &Connection, object: &mut rusqlite::Statement<'_>) -> Result<bool> {
    if !all_exist(
        object,
        [
            "evidence_tombstones",
            "consumer_cursors",
            "idx_evidence_tombstones_kind_revision",
            "observation_clock",
            "change_feed_store",
        ],
    )? {
        return Ok(false);
    }
    let identified: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM change_feed_store WHERE singleton = 1)",
        [],
        |row| row.get(0),
    )?;
    if !identified {
        return Ok(false);
    }
    let fingerprinted: bool = conn.query_row(
        &format!(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('change_feed_store') WHERE name='{EXPORT_SCHEMA_DIGEST_COLUMN}')"
        ),
        [],
        |row| row.get(0),
    )?;
    if !fingerprinted {
        return Ok(false);
    }
    let fingerprinted: bool = conn.query_row(
        &format!(
            "SELECT {EXPORT_SCHEMA_DIGEST_COLUMN} IS NOT NULL FROM change_feed_store WHERE singleton=1"
        ),
        [],
        |row| row.get(0),
    )?;
    Ok(fingerprinted)
}

/// Whether `kind`'s table is fed: its revision index, its stamping triggers
/// and its revision column.
fn kind_feed_is_current(
    conn: &Connection,
    object: &mut rusqlite::Statement<'_>,
    kind: ChangeKind,
) -> Result<bool> {
    let table = kind.table();
    if !object.exists([table.revision_index()])? {
        return Ok(false);
    }
    let [insert, update, delete] = trigger_names(kind);
    if !all_exist(object, [insert, delete])? {
        return Ok(false);
    }
    // The update trigger must also guard on the table's columns as they
    // are now: one built before a column was added would let an update
    // that changes only that column go unreported.
    if !update_trigger_is_current(conn, &update, &update_guard(conn, table.name)?)? {
        return Ok(false);
    }
    let stamped: bool = conn.query_row(
        &format!(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('{}') WHERE name = ?)",
            table.name
        ),
        [REVISION_COLUMN],
        |row| row.get(0),
    )?;
    Ok(stamped)
}

/// Add the revision columns, tombstone and cursor tables, indexes and the
/// stamping triggers, and stamp every row that predates them.
///
/// Runs inside `init_db`'s serialized migration pass, after the observation
/// schema has created the clock the triggers draw from. Idempotent: a current
/// database passes through on `IF NOT EXISTS` checks and one marker read.
pub(crate) fn init_schema(conn: &Connection) -> Result<()> {
    let identity_existed = feed_identity_exists(conn)?;
    create_feed_tables(conn)?;
    let backfill = !migration_applied(conn, MIGRATION)?;
    if backfill {
        // A database from before presences were fed: its session re-stamp
        // triggers hold the names the presence kind's own triggers take
        // below, and fire on the backfill's stamp. Both sets are created
        // afresh after the backfill.
        for trigger in RETIRED_PRESENCE_TRIGGERS {
            conn.execute_batch(&format!("DROP TRIGGER IF EXISTS {trigger};"))?;
        }
    }
    for kind in ChangeKind::ALL {
        feed_table(conn, *kind)?;
    }
    create_presence_triggers(conn)?;
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (name) VALUES (?)",
        [MIGRATION],
    )?;
    reconcile_export_schema(conn, identity_existed)?;
    Ok(())
}

/// The tombstone, cursor and identity tables, and the identity row.
fn create_feed_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS evidence_tombstones (
             kind TEXT NOT NULL,
             source TEXT NOT NULL,
             session_id TEXT NOT NULL,
             record_key TEXT NOT NULL,
             revision INTEGER NOT NULL,
             PRIMARY KEY (kind, source, session_id, record_key)
         );
         CREATE INDEX IF NOT EXISTS idx_evidence_tombstones_kind_revision \
             ON evidence_tombstones(kind, revision);
         CREATE TABLE IF NOT EXISTS consumer_cursors (
             name TEXT PRIMARY KEY,
             revision INTEGER NOT NULL,
             updated_ms INTEGER NOT NULL,
             kinds TEXT NOT NULL DEFAULT '*'
         );
         CREATE TABLE IF NOT EXISTS change_feed_store (
             singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
             epoch INTEGER NOT NULL,
             export_schema_digest TEXT
         );
         CREATE TABLE IF NOT EXISTS change_feed_epochs (
             epoch INTEGER PRIMARY KEY,
             successor INTEGER NOT NULL,
             through_revision INTEGER NOT NULL,
             retired_kinds TEXT NOT NULL
         );",
    )?;
    // The database's identity in every watermark it issues; see
    // `Watermark::epoch`. A nonzero random value, so a fresh database -- which
    // counts its revisions from zero again -- cannot pass for the one it
    // replaced. Export-schema changes keep this identity and restamp only the
    // affected rows above the current head.
    conn.execute(
        "INSERT OR IGNORE INTO change_feed_store (singleton, epoch) VALUES (1, random() | 1)",
        [],
    )?;
    ensure_columns(
        conn,
        "change_feed_store",
        &[(EXPORT_SCHEMA_DIGEST_COLUMN, "TEXT")],
    )?;
    // A cursor is a position in one kind set's stream; see `KindSet`. A
    // database that created the table before the column existed gains it
    // here, and both paths converge.
    ensure_columns(
        conn,
        "consumer_cursors",
        &[(KINDS_COLUMN, "TEXT NOT NULL DEFAULT '*'")],
    )?;
    Ok(())
}

/// Feed one kind's table: its revision column (stamping rows that predate
/// it), revision index and stamping triggers.
fn feed_table(conn: &Connection, kind: ChangeKind) -> Result<()> {
    let table = kind.table();
    ensure_columns(
        conn,
        table.name,
        &[(REVISION_COLUMN, "INTEGER NOT NULL DEFAULT 0")],
    )?;
    // Rows written before their table was fed are stamped once, in rowid
    // order, each above everything stamped before it. This is deliberately
    // not gated only by the fixed `change_feed_v2` marker: a later release
    // can add a ChangeKind to an already-v2 store. Existing fed tables have
    // no zero revisions, so reopening them is a no-op; a newly fed table's
    // existing rows become visible above the old head without rotating the
    // origin. A revision-only UPDATE does not fire the feed update trigger.
    conn.execute(
        &format!(
            "UPDATE {name} SET {REVISION_COLUMN} = rowid + \
             (SELECT version FROM observation_clock WHERE singleton = 1) \
             WHERE {REVISION_COLUMN} = 0",
            name = table.name
        ),
        [],
    )?;
    conn.execute(
        &format!(
            "UPDATE observation_clock SET version = MAX(version, \
             COALESCE((SELECT MAX({REVISION_COLUMN}) FROM {name}), 0)) \
             WHERE singleton = 1",
            name = table.name
        ),
        [],
    )?;
    conn.execute(
        &format!(
            "CREATE INDEX IF NOT EXISTS {index} ON {name}({REVISION_COLUMN})",
            index = table.revision_index(),
            name = table.name
        ),
        [],
    )?;
    let [insert, update, delete] = trigger_names(kind);
    let name = table.name;
    let kind = kind.as_str();
    let new_source = table.source_sql("NEW");
    let old_source = table.source_sql("OLD");
    let new_session = table.tombstone_session_sql("NEW");
    let old_session = table.tombstone_session_sql("OLD");
    let new_record = table.record_key_sql("NEW");
    let old_record = table.record_key_sql("OLD");
    let moved = table.identity_changed_sql();
    // The update trigger's own stamp changes `revision`, and only that,
    // so `NEW.revision = OLD.revision` is what stops it re-firing under
    // `recursive_triggers`; the rest of the guard is what makes an
    // external write that leaves the stamp alone (every upsert in this
    // crate) take a new one only when it changed a stamped column. See
    // `update_guard`. A change of identity is a delete of the old one and
    // an upsert of the new, each at its own revision.
    //
    // The guard is generated from the live column list, so a trigger
    // built before a column was added -- or before the guard existed --
    // is rebuilt here rather than kept by `IF NOT EXISTS`.
    let guard = update_guard(conn, name)?;
    if !update_trigger_is_current(conn, &update, &guard)? {
        conn.execute_batch(&format!("DROP TRIGGER IF EXISTS {update};"))?;
    }
    conn.execute_batch(&format!(
        "CREATE TRIGGER IF NOT EXISTS {insert} AFTER INSERT ON {name} BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE {name} SET {REVISION_COLUMN} = \
                 (SELECT version FROM observation_clock WHERE singleton = 1) \
                 WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = '{kind}' \
                 AND source = {new_source} AND session_id = {new_session} \
                 AND record_key = {new_record};
         END;
         CREATE TRIGGER IF NOT EXISTS {update} AFTER UPDATE ON {name}
         {guard} BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 \
                 AND ({moved});
             INSERT OR REPLACE INTO evidence_tombstones \
                 (kind, source, session_id, record_key, revision) \
                 SELECT '{kind}', {old_source}, {old_session}, {old_record}, \
                     (SELECT version FROM observation_clock WHERE singleton = 1) \
                 WHERE {moved};
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE {name} SET {REVISION_COLUMN} = \
                 (SELECT version FROM observation_clock WHERE singleton = 1) \
                 WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = '{kind}' \
                 AND source = {new_source} AND session_id = {new_session} \
                 AND record_key = {new_record};
         END;
         CREATE TRIGGER IF NOT EXISTS {delete} AFTER DELETE ON {name} BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones \
                 (kind, source, session_id, record_key, revision) \
                 VALUES ('{kind}', {old_source}, {old_session}, {old_record}, \
                     (SELECT version FROM observation_clock WHERE singleton = 1));
         END;"
    ))?;
    Ok(())
}

/// The triggers that re-stamp a session row when one of its presences comes,
/// goes or moves.
fn create_presence_triggers(conn: &Connection) -> Result<()> {
    // A direct write of `revision` does not re-fire the sessions update
    // trigger (its guard requires `NEW.revision = OLD.revision`), so this is
    // one stamp, not two. The update trigger carries the same guard, so a
    // presence's own stamp does not re-stamp its session a second time, and
    // it fires only when the presence's identity moves (see
    // `PRESENCE_MOVED_GUARD`): a key change is a presence leaving one session
    // or location and arriving at another, and both sessions are stamped.
    let stamp_session = |row: &str| {
        format!(
            "UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE sessions SET {REVISION_COLUMN} = \
                 (SELECT version FROM observation_clock WHERE singleton = 1) \
                 WHERE source = {row}.source AND session_id = {row}.session_id;"
        )
    };
    let stamp_new = stamp_session("NEW");
    let stamp_old = stamp_session("OLD");
    let [insert, update, delete] = PRESENCE_TRIGGERS else {
        unreachable!("three presence triggers")
    };
    if !update_trigger_is_current(conn, update, PRESENCE_MOVED_GUARD)? {
        conn.execute_batch(&format!("DROP TRIGGER IF EXISTS {update};"))?;
    }
    conn.execute_batch(&format!(
        "CREATE TRIGGER IF NOT EXISTS {insert} \
             AFTER INSERT ON session_presences BEGIN
             {stamp_new}
         END;
         CREATE TRIGGER IF NOT EXISTS {update} \
             AFTER UPDATE ON session_presences \
             {PRESENCE_MOVED_GUARD} BEGIN
             {stamp_new}
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 \
                 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id);
             UPDATE sessions SET {REVISION_COLUMN} = \
                 (SELECT version FROM observation_clock WHERE singleton = 1) \
                 WHERE source = OLD.source AND session_id = OLD.session_id \
                 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id);
         END;
         CREATE TRIGGER IF NOT EXISTS {delete} \
             AFTER DELETE ON session_presences BEGIN
             {stamp_old}
         END;"
    ))?;
    Ok(())
}
