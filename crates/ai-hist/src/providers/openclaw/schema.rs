//! Which agent-database layout a snapshot holds. Transcripts moved into the
//! agent database at schema 19 (OpenClaw 2026.8.1); before that the database
//! held only auth profiles and caches beside the JSONL session store. Later
//! schemas add the features below as new tables and columns, so each is
//! detected by name rather than inferred from the version.
use super::*;
use anyhow::{ensure, Context};
use rusqlite::OptionalExtension;

/// Released writers, verified against each one's own DDL: 19 (2026.8.1 to
/// 2026.9.4), 21 (2026.9.5), 23 (2026.9.6), 24 (2026.9.7 to 2026.10.4).
/// 20 and 22 were only ever development schemas between those releases; they
/// differ from their neighbours in the same detected features. 25-31 only add
/// columns and indexes outside the transcript tables this reader selects by
/// name.
pub(super) const SUPPORTED: std::ops::RangeInclusive<i64> = 19..=31;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Schema {
    pub version: i64,
    /// `transcript_events` and its projections exist (schema 19 onward). A
    /// pre-transcript database holds no sessions: they are still JSONL.
    pub transcripts: bool,
    /// `transcript_events.event_zstd` / `event_utf8_bytes` (schema 23 onward).
    pub zstd: bool,
    /// `session_transcript_cold_archives` (schema 21 onward).
    pub cold: bool,
}

fn has_table(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
        [name],
        |r| r.get(0),
    )?)
}
fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name=?2)",
        [table, column],
        |r| r.get(0),
    )?)
}

/// Validate one agent database's identity and report its layout.
pub(super) fn detect(conn: &Connection, agent: &str) -> Result<Schema> {
    ensure!(
        !has_table(conn, "openclaw_engine_anchor")?,
        "OpenClaw agent {agent:?} stores sessions in PostgreSQL; only SQLite agent databases are read"
    );
    let meta: Option<(String, i64, Option<String>)> = conn
        .query_row(
            "SELECT role, schema_version, agent_id FROM schema_meta WHERE meta_key='primary'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .context("unsupported OpenClaw agent database: missing schema_meta")?;
    let (role, version, owner) =
        meta.context("unsupported OpenClaw agent database: no primary schema_meta row")?;
    ensure!(
        role == "agent",
        "OpenClaw database role {role:?} is not an agent database"
    );
    // Upstream refuses a database whose recorded owner is another agent.
    ensure!(
        owner.as_deref().is_none_or(|owner| owner == agent),
        "OpenClaw database under agents/{agent} belongs to agent {owner:?}"
    );
    let user_version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    ensure!(
        user_version == version,
        "OpenClaw agent database is mid-migration (user_version {user_version}, schema_meta {version})"
    );
    let transcripts = has_table(conn, "transcript_events")?;
    // A database from before transcripts moved in, with none of their
    // tables, holds no sessions. Anything else outside the verified range is
    // refused rather than guessed at.
    let pre_transcript = version < *SUPPORTED.start() && version >= 1 && !transcripts;
    ensure!(
        pre_transcript || (SUPPORTED.contains(&version) && transcripts),
        "unsupported OpenClaw agent schema {version}; supported: {}-{}",
        SUPPORTED.start(),
        SUPPORTED.end()
    );
    Ok(Schema {
        version,
        transcripts,
        zstd: transcripts && has_column(conn, "transcript_events", "event_zstd")?,
        cold: has_table(conn, "session_transcript_cold_archives")?,
    })
}

impl Schema {
    /// The compressed payload and its length, or NULLs before schema 23.
    pub(super) fn payload_columns(&self) -> &'static str {
        if self.zstd {
            "e.event_zstd, e.event_utf8_bytes"
        } else {
            "NULL, NULL"
        }
    }
    /// The cold-archive table, or an empty stand-in with its columns.
    pub(super) fn cold_archives(&self) -> &'static str {
        if self.cold {
            "session_transcript_cold_archives"
        } else {
            "(SELECT NULL AS session_id, NULL AS generation, NULL AS archive_name, \
             NULL AS archive_sha256, NULL AS event_count, NULL AS raw_bytes, \
             NULL AS archive_bytes, NULL AS last_seq, NULL AS archived_at, NULL AS storage \
             WHERE 0)"
        }
    }
}
