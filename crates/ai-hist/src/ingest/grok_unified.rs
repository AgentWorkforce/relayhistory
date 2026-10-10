//! Grok's process-wide `logs/unified.jsonl`: reading it from a byte cursor
//! into `grok_unified_usage`, and attaching those rows to their sessions as
//! per-inference usage events.

use super::*;

/// Where Grok writes its process-wide per-inference log, under the Grok home
/// (`GROK_HOME`, else `~/.grok`).
pub(crate) fn grok_unified_log_path(grok_home: &Path) -> PathBuf {
    grok_home.join("logs").join("unified.jsonl")
}

/// The key the unified log's per-process model memory is parked under in its
/// `transcript_cursors` document.
const GROK_UNIFIED_CURSOR_KEY: &str = "grok_unified";

/// What one pass over `logs/unified.jsonl` did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct GrokUnifiedPass {
    /// Bytes read past the committed cursor. Zero when nothing was appended.
    pub bytes_read: u64,
    /// Usage rows read this pass, and how many of them were new.
    pub usage_rows: usize,
    pub new_rows: usize,
    /// Usage rows that named no session, so could not be attached.
    pub unattributed: usize,
    /// Terminated rows that were not JSON, or were too large to hold.
    pub malformed: usize,
    /// Catalogued sessions that gained usage events.
    pub sessions_materialized: usize,
    /// Stored rows, from any pass, whose session is not in the catalog. They
    /// are kept — the log is never re-read, so dropping them would lose the
    /// usage for good if the session is indexed later — and counted so the
    /// retained backlog is visible.
    pub orphan_rows: usize,
}

/// Read what `logs/unified.jsonl` gained since the last pass, in one
/// transaction: the new rows, the advanced cursor, and the usage events of
/// every catalogued session they touch commit together or not at all.
///
/// The transaction is taken `IMMEDIATE`, so the write lock is held from the
/// first read of the cursor: a deferred one that read the cursor while a
/// hydration was writing would fail to upgrade (`SQLITE_BUSY_SNAPSHOT`)
/// instead of waiting behind it.
pub(crate) fn sync_grok_unified_log(
    conn: &Connection,
    grok_home: &Path,
) -> Result<GrokUnifiedPass> {
    let tx = rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
    let pass = sync_grok_unified_log_in(&tx, grok_home)?;
    tx.commit()?;
    Ok(pass)
}

/// [`sync_grok_unified_log`] inside a transaction the caller already holds —
/// targeted hydration's.
///
/// The log is append-only and covers every session on the machine, so it is
/// read from a byte cursor, never re-read whole: a second pass over an
/// unchanged log reads zero bytes. Each usage row is copied into
/// `grok_unified_usage` — the durable copy, because the bytes are never read
/// again — keyed on [`grok::unified_row_key`], so a region read twice after a
/// rotation writes the same rows. A trailing line with no newline is withheld
/// until it has one. A terminated row that is not JSON is counted and passed
/// over: the log is Grok's process log, not a session's evidence, and holding
/// the cursor on one bad line would stop every later inference being read.
pub(crate) fn sync_grok_unified_log_in(
    conn: &Connection,
    grok_home: &Path,
) -> Result<GrokUnifiedPass> {
    let mut pass = GrokUnifiedPass::default();
    let path = grok_unified_log_path(grok_home);
    if !grok_unified_log_exists(&path)? {
        return Ok(pass);
    }
    let locator = path.to_string_lossy().to_string();
    let key = transcript_cursor::CursorKey::Locator {
        source: "grok",
        locator: &locator,
    };
    let mut cursor = transcript_cursor::load_cursor(conn, &key)?;
    let mut reader = transcript_cursor::TranscriptReader::open(&path, cursor.file.as_ref(), None)
        .with_context(|| format!("read Grok unified log {}", path.display()))?;
    let start = reader.start_offset();
    // Which model each process last named. Only meaningful for the bytes
    // after the cursor, so a log read from the start begins with none.
    let mut pid_models: HashMap<i64, String> = if start == 0 {
        HashMap::new()
    } else {
        cursor
            .extra
            .get(GROK_UNIFIED_CURSOR_KEY)
            .and_then(|state| state.get("pid_models"))
            .and_then(|models| serde_json::from_value(models.clone()).ok())
            .unwrap_or_default()
    };
    let mut touched: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut line = String::new();
    loop {
        check_capture_cancelled()?;
        let offset = reader.position();
        match reader.next_line(&mut line)? {
            None | Some(transcript_cursor::ReadRecord::Unterminated) => break,
            Some(transcript_cursor::ReadRecord::Oversized { .. }) => {
                pass.malformed += 1;
                continue;
            }
            Some(transcript_cursor::ReadRecord::Terminated) => {}
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            pass.malformed += 1;
            continue;
        };
        match grok::parse_unified_row(&value, &pid_models) {
            grok::GrokUnifiedRow::Usage(usage) => {
                pass.usage_rows += 1;
                pass.new_rows += insert_grok_unified_usage(conn, &value, &usage, &locator, offset)?;
                touched.insert(usage.session_id);
            }
            grok::GrokUnifiedRow::ModelChange { pid, model } => {
                pid_models.insert(pid, model);
            }
            grok::GrokUnifiedRow::Unattributed => pass.unattributed += 1,
            grok::GrokUnifiedRow::Other => {}
        }
    }
    pass.bytes_read = reader.position().saturating_sub(start);
    match reader.commit(reader.position())? {
        transcript_cursor::CommitOutcome::Published(file) => {
            cursor.file = Some(file);
            cursor.extra.insert(
                GROK_UNIFIED_CURSOR_KEY.to_string(),
                json!({ "pid_models": pid_models }),
            );
            transcript_cursor::store_cursor(conn, &key, &cursor)?;
        }
        // The log was replaced or truncated while it was being read. The rows
        // already copied are keyed on their content, so keeping them is
        // harmless; the cursor stays where it was and the next pass re-reads.
        transcript_cursor::CommitOutcome::Superseded => {}
    }
    for session_id in touched {
        if materialize_unified_usage_into(conn, &session_id)? {
            pass.sessions_materialized += 1;
        }
    }
    let orphans: i64 = conn.query_row(
        "SELECT COUNT(*) FROM grok_unified_usage u WHERE NOT EXISTS \
         (SELECT 1 FROM sessions s WHERE s.source = 'grok' AND s.session_id = u.session_id)",
        [],
        |row| row.get(0),
    )?;
    pass.orphan_rows = orphans as usize;
    Ok(pass)
}

/// Copies one unified-log usage row into `grok_unified_usage`; the rows it
/// added (none when the row was already there).
fn insert_grok_unified_usage(
    conn: &Connection,
    value: &Value,
    usage: &grok::GrokUnifiedUsage,
    locator: &str,
    offset: u64,
) -> Result<usize> {
    Ok(conn.execute(
        "INSERT OR IGNORE INTO grok_unified_usage \
         (row_key, session_id, ts_ms, pid, model, event_id, usage_json, locator, line_offset) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            grok::unified_row_key(value),
            usage.session_id,
            usage.ts_ms,
            usage.pid,
            usage.model,
            usage.event_id,
            usage.usage.to_string(),
            locator,
            offset as i64,
        ],
    )?)
}

/// Attaches a session's waiting unified-log usage to it once it is indexed;
/// `true` when that added any.
fn materialize_unified_usage_into(conn: &Connection, session_id: &str) -> Result<bool> {
    if !grok_catalog_row_exists(conn, session_id)? {
        // Retained, not dropped: the rows wait in `grok_unified_usage`
        // until the session is indexed, and that ingestion attaches them.
        return Ok(false);
    }
    if materialize_grok_unified_usage(conn, session_id, None)?.inserted == 0 {
        return Ok(false);
    }
    // A cached hydration replays the diagnostics it stored, and those
    // were written before this usage existed. Clearing them makes the
    // next unchanged read rebuild the usage caveat from the stored rows.
    conn.execute(
        "UPDATE session_hydration_checkpoints SET source_diagnostics_json = NULL \
         WHERE source = 'grok' AND session_id = ?",
        params![session_id],
    )?;
    Ok(true)
}

/// Whether Grok's unified log is a file to read; a missing one is not an error.
fn grok_unified_log_exists(path: &Path) -> Result<bool> {
    match path.metadata() {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("stat Grok unified log {}", path.display()))
        }
    }
}

/// How many `logs/unified.jsonl` usage rows are stored for one session.
fn grok_unified_rows_pending(conn: &Connection, session_id: &str) -> Result<usize> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM grok_unified_usage WHERE session_id = ?",
        params![session_id],
        |row| row.get(0),
    )?;
    Ok(count as usize)
}

/// What attaching one session's `logs/unified.jsonl` rows established.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GrokUnifiedCoverage {
    /// Stored usage rows for the session, attached now or before.
    pub rows: usize,
    /// Rows attached by this call; zero when nothing was new.
    pub inserted: usize,
    /// Turns the log covers: a demoted breakdown, or any other turn whose
    /// window holds a log row (including a context snapshot with no breakdown).
    pub covered_turns: usize,
    /// Turns the log does not cover whose `turn_completed.usage` still counts.
    pub turn_usage_turns: usize,
    /// Turns the log does not cover and that carry no breakdown: a context
    /// snapshot, or a turn the census recorded that left no row.
    pub proxy_only_turns: usize,
}

/// How many turns the last replacing read of one Grok session saw.
pub(super) fn store_grok_turn_census(
    conn: &Connection,
    session_id: &str,
    turns: usize,
) -> Result<()> {
    conn.execute(
        "INSERT INTO grok_session_turns (session_id, turns) VALUES (?, ?) \
         ON CONFLICT(session_id) DO UPDATE SET turns = excluded.turns",
        params![session_id, turns as i64],
    )?;
    Ok(())
}

pub(super) fn grok_turn_census(conn: &Connection, session_id: &str) -> Result<Option<usize>> {
    let turns: Option<i64> = conn
        .query_row(
            "SELECT turns FROM grok_session_turns WHERE session_id = ?",
            params![session_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(turns.map(|turns| turns.max(0) as usize))
}

/// Whether one stored turn row's window contains a unified-log timestamp.
fn unified_timestamp_covers(token: &Value, timestamps: &[Option<i64>]) -> bool {
    let Some(start) = token.get("turn_start_ms").and_then(Value::as_i64) else {
        return false;
    };
    let Some(end) = token.get("turn_end_ms").and_then(Value::as_i64) else {
        return false;
    };
    timestamps
        .iter()
        .flatten()
        .any(|ts| *ts >= start && *ts <= end)
}

/// Which of a session's turns `logs/unified.jsonl` covers.
///
/// `turn_tokens` are the `updates.jsonl` token objects, one per turn that
/// left a row. `unified_timestamps` are every stored log row's time (`None`
/// when the row named none — that row covers every turn). `census_turns` is
/// how many turns the stream opened, when the replacing read recorded it;
/// turns it counts that left no row are uncovered unless a timeless log row
/// covers the session.
pub(crate) fn classify_grok_turn_coverage(
    turn_tokens: &[Value],
    unified_timestamps: &[Option<i64>],
    census_turns: Option<usize>,
) -> GrokUnifiedCoverage {
    let timeless = unified_timestamps.iter().any(Option::is_none);
    let mut covered = 0usize;
    let mut own = 0usize;
    let mut proxy = 0usize;
    for token in turn_tokens {
        if timeless
            || token.get("turn_usage").is_some()
            || unified_timestamp_covers(token, unified_timestamps)
        {
            covered += 1;
        } else if token.get("usage").is_some() {
            own += 1;
        } else {
            proxy += 1;
        }
    }
    if let Some(turns) = census_turns {
        let hidden = turns.saturating_sub(covered + own + proxy);
        if timeless {
            covered += hidden;
        } else {
            proxy += hidden;
        }
    }
    GrokUnifiedCoverage {
        covered_turns: covered,
        turn_usage_turns: own,
        proxy_only_turns: proxy,
        ..GrokUnifiedCoverage::default()
    }
}

/// One stored `grok_unified_usage` row, as materialization reads it.
struct GrokUnifiedStored {
    row_key: String,
    ts_ms: Option<i64>,
    pid: Option<i64>,
    model: Option<String>,
    event_id: Option<String>,
    usage_json: String,
}

impl GrokUnifiedStored {
    /// The `token_json` of the usage event this row becomes.
    fn token_json(&self) -> String {
        let mut token_json = serde_json::Map::new();
        token_json.insert(
            "usage".into(),
            serde_json::from_str(&self.usage_json).unwrap_or(Value::Null),
        );
        token_json.insert("source".into(), json!("logs/unified.jsonl"));
        if let Some(pid) = self.pid {
            token_json.insert("pid".into(), json!(pid));
        }
        if let Some(event_id) = &self.event_id {
            token_json.insert("event_id".into(), json!(event_id));
        }
        Value::Object(token_json).to_string()
    }
}

/// Attach one session's stored `logs/unified.jsonl` rows as usage events, and
/// settle which of its turns they cover.
///
/// Each row becomes one assistant event keyed `unified:<row key>`, in its own
/// request span, with `token_json = {"usage": <counters>, "source":
/// "logs/unified.jsonl", …}` — one inference, one request, normalized as
/// `per-request` usage. The event carries no text, so it is
/// `kind = "text"` with `raw_kind = "unified_log_usage"` to say what it is.
/// Only rows with no event yet are inserted: an append attaches its own rows
/// and leaves every event already stored untouched, so the change feed sees
/// the new rows and nothing else.
///
/// Coverage is decided **per turn**. A turn's `turn_completed.usage` is moved
/// to `turn_usage` (not normalized) only when a log row falls inside that
/// turn's `[turn_start_ms, turn_end_ms]`; a turn the log does not reach —
/// the log started mid-session, or was rotated away — keeps its breakdown as
/// `usage`. So the same spend is never counted twice and a turn the log never
/// saw is not dropped. A log row with no time cannot be placed in a turn, so
/// its presence covers every turn: dropping a turn's breakdown is recoverable
/// from `turn_usage`, counting one inference twice is not. The rows' models
/// join `models_json`, and a row later than the session's last activity
/// extends it.
pub(super) fn materialize_grok_unified_usage(
    conn: &Connection,
    session_id: &str,
    fallback_ts: Option<i64>,
) -> Result<GrokUnifiedCoverage> {
    const SOURCE: &str = "grok";
    type SessionFacts = (Option<String>, Option<String>, Option<i64>, Option<String>);
    let session: Option<SessionFacts> = conn
        .query_row(
            "SELECT cwd, git_branch, first_activity_ms, models_json FROM sessions \
             WHERE source = 'grok' AND session_id = ?",
            params![session_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((cwd, branch, first_activity, models_json)) = session else {
        return Ok(GrokUnifiedCoverage::default());
    };
    let total = grok_unified_rows_pending(conn, session_id)?;
    if total == 0 {
        return Ok(GrokUnifiedCoverage::default());
    }
    let rows = grok_unified_rows_unattached(conn, session_id)?;
    let fallback_ts = fallback_ts.or(first_activity).unwrap_or(0);
    let mut models: Vec<String> = models_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default();
    let models_before = models.len();
    for row in &rows {
        check_capture_cancelled()?;
        let uid = format!("unified:{}", row.row_key);
        let token_json = row.token_json();
        insert_session_event(
            conn,
            &EventRow {
                source: SOURCE,
                session_id,
                project: cwd.as_deref(),
                cwd: cwd.as_deref(),
                git_branch: branch.as_deref(),
                message_id: &uid,
                ts_ms: row.ts_ms.unwrap_or(fallback_ts),
                role: "assistant",
                kind: "text",
                model: row.model.as_deref(),
                token_json: Some(&token_json),
                event_uid: &uid,
                raw_facts: RawMessageFacts {
                    request_span: Some(&uid),
                    ..RawMessageFacts::default()
                },
                raw_kind: Some("unified_log_usage"),
                ..EventRow::default()
            },
        )?;
        if let Some(model) = &row.model {
            note_model(&mut models, model);
        }
    }
    settle_grok_unified_session(conn, session_id, &models, models_before)?;
    let class = grok_turn_coverage(conn, session_id)?;
    Ok(GrokUnifiedCoverage {
        rows: total,
        inserted: rows.len(),
        covered_turns: class.covered_turns,
        turn_usage_turns: class.turn_usage_turns,
        proxy_only_turns: class.proxy_only_turns,
    })
}

/// A session's stored unified-log rows whose local event is not stored yet.
/// A row already attached is left exactly as it is.
fn grok_unified_rows_unattached(
    conn: &Connection,
    session_id: &str,
) -> Result<Vec<GrokUnifiedStored>> {
    Ok(conn
        .prepare(
            "SELECT row_key, ts_ms, pid, model, event_id, usage_json FROM grok_unified_usage u \
             WHERE session_id = ?1 AND NOT EXISTS (SELECT 1 FROM session_events e \
               WHERE e.source = 'grok' AND e.session_id = ?1 \
               AND e.event_uid = 'unified:' || u.row_key AND e.location IN ('local', 'both')) \
             ORDER BY ts_ms IS NULL, ts_ms, locator, line_offset, row_key",
        )?
        .query_map(params![session_id], |row| {
            Ok(GrokUnifiedStored {
                row_key: row.get(0)?,
                ts_ms: row.get(1)?,
                pid: row.get(2)?,
                model: row.get(3)?,
                event_id: row.get(4)?,
                usage_json: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Settles a session once its unified-log usage events are written: demotes
/// the turn breakdowns the log reaches into, records any model it newly names
/// and extends its last activity.
fn settle_grok_unified_session(
    conn: &Connection,
    session_id: &str,
    models: &[String],
    models_before: usize,
) -> Result<()> {
    // Per turn: demote a turn's breakdown only when the log reaches into it.
    // A turn with no recorded window cannot be placed, so a timed row never
    // covers it; only a timeless row (which covers every turn) does. Otherwise
    // it keeps its own `usage`, and `classify_grok_turn_coverage` counts it so.
    conn.execute(
        "UPDATE session_events SET token_json = json_set(json_remove(token_json, '$.usage'), \
         '$.turn_usage', json(json_extract(token_json, '$.usage'))) \
         WHERE source = 'grok' AND session_id = ?1 AND event_uid NOT LIKE 'unified:%' \
         AND token_json IS NOT NULL AND json_valid(token_json) \
         AND json_extract(token_json, '$.source') = 'updates.jsonl' \
         AND json_type(token_json, '$.usage') IS NOT NULL \
         AND (EXISTS (SELECT 1 FROM grok_unified_usage u WHERE u.session_id = ?1 \
                AND u.ts_ms IS NULL) \
           OR EXISTS (SELECT 1 FROM grok_unified_usage u WHERE u.session_id = ?1 \
                AND u.ts_ms BETWEEN json_extract(token_json, '$.turn_start_ms') \
                                AND json_extract(token_json, '$.turn_end_ms')))",
        params![session_id],
    )?;
    if models.len() != models_before {
        conn.execute(
            "UPDATE sessions SET models_json = ? WHERE source = 'grok' AND session_id = ?",
            params![serde_json::to_string(models)?, session_id],
        )?;
    }
    // An inference logged after the session's last recorded activity is
    // activity too.
    conn.execute(
        "UPDATE sessions SET last_activity_ms = \
           (SELECT MAX(ts_ms) FROM grok_unified_usage WHERE session_id = ?1) \
         WHERE source = 'grok' AND session_id = ?1 \
         AND (SELECT MAX(ts_ms) FROM grok_unified_usage WHERE session_id = ?1) \
             > COALESCE(last_activity_ms, 0)",
        params![session_id],
    )?;
    Ok(())
}

/// Coverage of one session from the rows already stored: the turn token
/// objects, the log timestamps, and the turn census when a replacing read
/// wrote one.
fn grok_turn_coverage(conn: &Connection, session_id: &str) -> Result<GrokUnifiedCoverage> {
    let turn_tokens = conn
        .prepare(
            "SELECT token_json FROM session_events \
             WHERE source = 'grok' AND session_id = ? \
               AND token_json IS NOT NULL AND json_valid(token_json) \
               AND json_extract(token_json, '$.source') = 'updates.jsonl'",
        )?
        .query_map(params![session_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .filter_map(|raw| serde_json::from_str::<Value>(raw).ok())
        .collect::<Vec<_>>();
    let unified_timestamps = conn
        .prepare("SELECT ts_ms FROM grok_unified_usage WHERE session_id = ?")?
        .query_map(params![session_id], |row| row.get::<_, Option<i64>>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(classify_grok_turn_coverage(
        &turn_tokens,
        &unified_timestamps,
        grok_turn_census(conn, session_id)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_db;

    /// A timed log row covers only the turn whose window holds it. A turn
    /// with no recorded window keeps its own `usage` rather than losing it
    /// to a row that belongs to another turn; a timeless row still covers it.
    #[test]
    fn a_windowless_grok_turn_keeps_its_usage_unless_a_timeless_row_covers_it() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd) VALUES ('g', 'grok', '/tmp/g')",
            [],
        )
        .unwrap();
        let windowed = json!({
            "source": "updates.jsonl",
            "turn_start_ms": 10,
            "turn_end_ms": 20,
            "usage": {"inputTokens": 5}
        });
        let windowless = json!({
            "source": "updates.jsonl",
            "usage": {"inputTokens": 7}
        });
        for (uid, token) in [("t1", &windowed), ("t2", &windowless)] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, ts_ms, role, kind, event_uid, token_json) \
                 VALUES ('grok', 'g', 15, 'assistant', 'text', ?, ?)",
                params![uid, token.to_string()],
            )
            .unwrap();
        }
        let insert_row = |key: &str, ts: Option<i64>| {
            conn.execute(
                "INSERT INTO grok_unified_usage \
                 (row_key, session_id, ts_ms, usage_json, locator, line_offset) \
                 VALUES (?, 'g', ?, '{\"inputTokens\":3}', 'unified.jsonl', 0)",
                params![key, ts],
            )
            .unwrap();
        };
        let token = |uid: &str| -> Value {
            let raw: String = conn
                .query_row(
                    "SELECT token_json FROM session_events \
                     WHERE session_id = 'g' AND event_uid = ?",
                    params![uid],
                    |row| row.get(0),
                )
                .unwrap();
            serde_json::from_str(&raw).unwrap()
        };

        insert_row("timed", Some(15));
        let coverage = materialize_grok_unified_usage(&conn, "g", None).unwrap();
        assert!(token("t1").get("usage").is_none());
        assert!(token("t1").get("turn_usage").is_some());
        assert_eq!(
            token("t2")["usage"]["inputTokens"],
            7,
            "a windowless turn keeps its usage"
        );
        assert!(token("t2").get("turn_usage").is_none());
        assert_eq!(
            (
                coverage.covered_turns,
                coverage.turn_usage_turns,
                coverage.proxy_only_turns
            ),
            (1, 1, 0)
        );

        insert_row("timeless", None);
        let coverage = materialize_grok_unified_usage(&conn, "g", None).unwrap();
        assert!(token("t2").get("usage").is_none());
        assert_eq!(token("t2")["turn_usage"]["inputTokens"], 7);
        assert_eq!(
            (
                coverage.covered_turns,
                coverage.turn_usage_turns,
                coverage.proxy_only_turns
            ),
            (2, 0, 0)
        );
    }

    #[test]
    fn classify_grok_turn_coverage_counts_windows_census_and_timeless_rows() {
        let covered = json!({
            "source": "updates.jsonl",
            "turn_start_ms": 10,
            "turn_end_ms": 20,
            "context_total_tokens": 1
        });
        let own = json!({
            "source": "updates.jsonl",
            "turn_start_ms": 30,
            "turn_end_ms": 40,
            "usage": {"inputTokens": 1}
        });
        let proxy = json!({
            "source": "updates.jsonl",
            "context_total_tokens": 2,
            "turn_start_ms": 50,
            "turn_end_ms": 60
        });
        let class = classify_grok_turn_coverage(
            &[covered.clone(), own.clone(), proxy],
            &[Some(15)],
            Some(4),
        );
        assert_eq!(
            (
                class.covered_turns,
                class.turn_usage_turns,
                class.proxy_only_turns
            ),
            (1, 1, 2)
        );

        let demoted = json!({
            "source": "updates.jsonl",
            "turn_usage": {"inputTokens": 1},
            "turn_start_ms": 30
        });
        let timeless = classify_grok_turn_coverage(&[own, demoted], &[None], Some(3));
        assert_eq!(
            (
                timeless.covered_turns,
                timeless.turn_usage_turns,
                timeless.proxy_only_turns
            ),
            (3, 0, 0)
        );

        let no_census = classify_grok_turn_coverage(&[covered], &[Some(15)], None);
        assert_eq!(
            (
                no_census.covered_turns,
                no_census.turn_usage_turns,
                no_census.proxy_only_turns
            ),
            (1, 0, 0)
        );
    }
}
