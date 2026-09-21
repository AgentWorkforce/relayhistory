//! Targeted, provider-bounded session evidence acquisition.

use super::*;
use crate::observations::{self, ObservationCheckpoint, ObservationKey, SessionObservation};
use crate::source_evidence::EvidenceKind;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::time::Instant;

/// Bumped to 3 when capability became derived from declared evidence coverage.
pub const SESSION_HYDRATION_CONTRACT_VERSION: u32 = 3;
/// Bumped to 2 when Claude subagent transcripts that carry an `agentId`
/// started being indexed under that child id: existing databases re-parse once
/// and the earlier parent-attributed rows are healed in place.
///
/// Version 3 re-parsed existing sessions for per-message raw provider facts.
/// Version 4 also invalidates checkpoints from the unbounded Codex child scan,
/// so their relationship coverage is recomputed under the bounded search.
/// Version 5 adds tool-result fidelity and reattributes identified Claude
/// subagent events to their child sessions, and banks continuity evidence for
/// sessions checkpointed by version 4.
/// Version 6 re-parses Grok sessions that were prompt-only: they gain
/// `session_events`, `tool_calls`, `file_edits`, `session_markers`, subagent
/// relationships and the timestamps `updates.jsonl` recorded, in place of the
/// `created_at + index` times the previous parser synthesized. Plain `sync`
/// needs the same push, which is why the `grok_sessions` sync-state key was
/// retired for `grok_events_v1`.
///
/// Version 7 is the Cursor event-level parser that landed beside Grok's 6:
/// already indexed Cursor sessions re-parse once and gain `session_events`,
/// `tool_calls`, `file_edits` and real turn timestamps. The same bump carries
/// the fix that stopped a user-role record carrying only a `tool_result` from
/// closing the open turn — those records were dated with the file mtime, so an
/// already indexed Cursor session holds tool results timed hours after the
/// calls they answer, and session windows that were dragged to the mtime by
/// records that store no evidence at all. Neither heals in place — the window
/// only ever widens — so the transcript has to be read again. Plain `sync`
/// needs the same push, which is why `CURSOR_SYNC_STATE_KEY` was retired in
/// the same change: a parser version alone only reaches sessions somebody
/// hydrates by name. Databases that already ran Grok's 6 keep that number
/// until this bump, so Cursor would otherwise stay prompt-only forever.
///
/// Version 8 is usage grouping. It captures Claude's provider message id, so
/// records from transcripts without a request id can still be grouped per API
/// call, and Codex's `request_span`, so the rows of one Codex call group
/// together instead of each becoming a request of its own. Both live only in
/// the transcript, so an already indexed session keeps the old grouping until
/// it is read again.
///
/// This began as a second version 7, written before Cursor's landed on main.
/// Two different re-parses cannot share a number: a database that ran the
/// Cursor 7 would report the usage 7 as already done and keep answering with
/// the old grouping forever.
///
/// Version 9 extends `session_markers` to the Claude and Codex parsers, for
/// the record types they used to drop -- compaction and summary boundaries,
/// system rows, non-text content blocks and Codex lifecycle events -- and
/// carries Grok's own markers onto the merged marker model. A database
/// checkpointed at 8 or earlier has the Claude and Codex rows nowhere, and
/// nothing short of re-reading the transcript can recover them, so every
/// session re-parses once.
///
/// It is 9 for the third time of asking, and that is the point worth keeping.
/// It was 7 until Cursor's event-level parser took 7; 8 until usage grouping
/// took 8. Two branches that each need a one-time re-parse will both pick
/// `main + 1` while they are open, and the collision is an identical-line
/// change that merges cleanly and passes every gate -- each value is correct
/// in isolation, so there is nothing here for a test to catch. Whoever merges
/// main onto this branch next must re-read this number from main and take
/// main's plus one if it has moved again.
const HYDRATION_PARSER_VERSION: i64 = 9;

#[derive(Debug, Clone)]
pub struct HydrateSessionOptions {
    pub source: String,
    pub session_id: String,
    pub scope: SessionScope,
    pub include_related: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HydrationIndexedThrough {
    pub source_stamp: Option<String>,
    pub last_event_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HydrationEvidence {
    pub prompts: u64,
    pub events: u64,
    pub tool_calls: u64,
    pub file_edits: u64,
    pub related_sessions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrationDiagnostic {
    pub code: String,
    pub message: String,
    pub duration_ms: Option<i64>,
    pub source_bytes: Option<i64>,
    pub records_parsed: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HydrateSessionResult {
    pub contract_version: u32,
    pub source: String,
    pub session_id: String,
    pub status: String,
    /// `full` only when every kind in [`FULL_SESSION_KINDS`] is covered.
    /// Derived from [`coverage`](Self::coverage), never asserted.
    pub capability: String,
    pub discovery_state: String,
    pub presence: String,
    pub indexed_through: HydrationIndexedThrough,
    pub evidence: HydrationEvidence,
    /// The evidence kinds this hydration can have indexed, in canonical order.
    /// For a local session it is the provider adapter's declared coverage; for
    /// an acquired snapshot it is the connector's reported `covered_kinds`. A
    /// zero count for a *covered* kind means the session has none of it; an
    /// absent kind means nothing here ever looked.
    pub coverage: Vec<EvidenceKind>,
    pub related_session_ids: Vec<String>,
    pub diagnostics: Vec<HydrationDiagnostic>,
}

#[derive(Debug)]
struct CatalogTarget {
    locator: Option<String>,
    discovery_state: Option<String>,
}

#[derive(Debug)]
struct SourceSnapshot {
    stamp: String,
    bytes: i64,
    records: SnapshotRecords,
    path: Option<PathBuf>,
    /// Claude subagent sidecars, parsed once while stamping the source so the
    /// ingestion pass does not walk and re-parse the same files.
    claude_subagents: Vec<ClaudeSubagentEvidence>,
    /// A bounded Codex child search cannot assert complete relationship coverage
    /// when newer date directories exist beyond its search window.
    codex_relationship_complete: bool,
}

/// How many records a source holds, and whether counting them is free.
///
/// Counting means reading every byte of the source. For a single-file provider
/// that read has already happened by the time the snapshot exists, so the
/// number is simply carried. Grok's source is a directory whose update stream
/// dominates it, and the count is wanted only when the session is actually
/// going to be parsed — so it is deferred, and a run that decides nothing
/// changed never opens the files at all.
#[derive(Debug)]
enum SnapshotRecords {
    Counted(i64),
    DeferredGrok(PathBuf),
}

impl SnapshotRecords {
    /// The count, doing the content pass if it has not happened. Called only
    /// on the path that parses the session.
    fn count(&self) -> Result<i64> {
        match self {
            Self::Counted(records) => Ok(*records),
            Self::DeferredGrok(path) => grok_source_records(path),
        }
    }

    /// Add a sidecar's records to an already-counted source. A deferred count
    /// covers its own directory, so nothing is ever added to one.
    fn plus(self, more: i64) -> Self {
        match self {
            Self::Counted(records) => Self::Counted(records + more),
            deferred => deferred,
        }
    }
}

fn hydration_error(code: &str, message: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!("{code}: {message}")
}

pub fn hydrate_session(options: &HydrateSessionOptions) -> Result<HydrateSessionResult> {
    hydrate_session_at(&default_db_path(), options)
}

pub fn hydrate_session_at(
    db_path: &Path,
    options: &HydrateSessionOptions,
) -> Result<HydrateSessionResult> {
    hydrate_session_at_with_connectors(
        db_path,
        options,
        &crate::remote::SourceConnectorSelection::default(),
    )
}

pub fn hydrate_session_at_with_connectors(
    db_path: &Path,
    options: &HydrateSessionOptions,
    connectors: &crate::remote::SourceConnectorSelection,
) -> Result<HydrateSessionResult> {
    let roots = crate::ProviderRoots::from_env(home_dir());
    hydrate_session_at_with_roots_and_connectors(db_path, options, &roots, connectors)
}

#[cfg(test)]
fn hydrate_session_at_with_home(
    db_path: &Path,
    options: &HydrateSessionOptions,
    home: &Path,
) -> Result<HydrateSessionResult> {
    let roots = crate::ProviderRoots::from_home(
        home.to_path_buf(),
        home.join(".local/share/opencode/opencode.db"),
    );
    hydrate_session_at_with_roots_and_connectors(
        db_path,
        options,
        &roots,
        &crate::remote::SourceConnectorSelection::default(),
    )
}

#[cfg(test)]
fn test_sync_cursor(conn: &Connection, home: &Path, state: &mut Map<String, Value>) {
    super::sync_cursor(conn, state, &home.join(".cursor/projects")).unwrap();
}

#[cfg(test)]
fn test_load_sync_state(db_path: &Path) -> Map<String, Value> {
    super::load_sync_state(
        &db_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(".sync-state.json"),
    )
    .unwrap()
}

fn hydrate_session_at_with_roots_and_connectors(
    db_path: &Path,
    options: &HydrateSessionOptions,
    roots: &crate::ProviderRoots,
    connectors: &crate::remote::SourceConnectorSelection,
) -> Result<HydrateSessionResult> {
    let home = &roots.home;
    validate_options(options)?;
    // One hydration is one acquisition pass; see `begin_acquisition_pass`.
    crate::project_identity::begin_acquisition_pass();
    if options.scope == SessionScope::Remote {
        crate::remote::ensure_selected_remote_connectors_configured_for_at(
            "hydration",
            home,
            std::slice::from_ref(&options.source),
            connectors,
        )
        .map_err(|error| hydration_error("CONNECTOR_NOT_CONFIGURED", error))?;
    }
    let started = Instant::now();
    // Acquisition and replacement are one per-session critical section. The
    // provider call has to be inside it: otherwise an older response can wait
    // behind a newer writer and then replace that newer evidence. A file lock
    // covers both threads and independent RelayHistory processes.
    let _remote_lock = (options.scope == SessionScope::Remote)
        .then(|| acquire_remote_hydration_lock(db_path, options))
        .transpose()?;
    let mut conn = open_db(db_path)?;
    let mut target = catalog_target(&conn, options)?;
    if options.scope == SessionScope::Remote {
        return hydrate_remote_session(&mut conn, options, home, connectors, started);
    }
    let local_key = ObservationKey {
        source: options.source.clone(),
        session_id: options.session_id.clone(),
        location: SessionLocation::Local,
        connector_id: options.source.clone(),
        connector_instance: "default".into(),
    };
    let local_observation = observations::get(&conn, &local_key)?;
    if let Some(observation) = &local_observation {
        // OpenCode enumerates by session id, while its local parser needs the
        // separately recorded provider database path from the catalog.
        if options.source != "opencode" {
            target.locator = observation.raw_locator.clone();
        }
        target.discovery_state = Some(observation.discovery_state.clone());
    } else {
        anyhow::ensure!(
            !observations::list(&conn, &options.source, &options.session_id)?
                .iter()
                .any(
                    |observation| observation.key.location == SessionLocation::Local
                        && observation.key.connector_id != "legacy-unknown"
                ),
            "CONNECTOR_NOT_CONFIGURED: the builtin local adapter has not observed this session"
        );
    }
    let snapshot = source_snapshot(options, &target, roots)?;
    let previous = observations::checkpoint(&conn, &local_key)?.map(|checkpoint| {
        (
            checkpoint.source_stamp,
            checkpoint.parser_version,
            checkpoint.include_related,
        )
    });
    let previous_stamp = previous.as_ref().and_then(|(stamp, _, _)| stamp.clone());

    if previous_stamp.as_deref() == Some(snapshot.stamp.as_str())
        && previous
            .as_ref()
            .is_some_and(|(_, parser_version, _)| *parser_version == HYDRATION_PARSER_VERSION)
        && previous
            .as_ref()
            .is_some_and(|(_, _, included)| *included == options.include_related)
        && target.discovery_state.as_deref() == Some("full")
    {
        // What the provider's own records could not establish is a fact
        // about the stored evidence, not about this run. A reader of an
        // `unchanged` result is looking at exactly the rows the parse-path
        // reader saw, so it has to be told the same things about them --
        // above all that a token count it can see is a context proxy and not
        // billing usage.
        let cached_diagnostics = stored_source_diagnostics(&conn, options)?;
        // The record count comes from the checkpoint the last parse wrote:
        // this run parsed nothing, and counting the records again would mean
        // reading every file the short-circuit exists to skip. `bytes` is
        // metadata and stays current.
        let records_parsed = stored_records_parsed(&conn, options)?;
        return build_result_with(
            &conn,
            options,
            "unchanged",
            snapshot.stamp,
            snapshot.bytes,
            records_parsed,
            started.elapsed().as_millis() as i64,
            cached_diagnostics,
            snapshot.codex_relationship_complete,
        );
    }

    // The content pass, on the one path that has already read every one of
    // these files anyway. Taken *before* the writer lock: it re-reads every
    // byte of the session's files, and doing that inside the transaction
    // would hold the lock for the length of a full content pass over a
    // directory that may still be growing.
    let records_parsed = snapshot.records.count()?;

    // One selected provider session is one destination transaction. Provider
    // JSONL readers ignore an incomplete final record -- one that is not
    // newline-terminated, and so is still being written -- and every evidence
    // table has a provider-native uniqueness key, so interruption followed by
    // retry is safe for both new and growing sessions.
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (source_diagnostics, cursor_consumed_through) = ingest_selected(
        &tx,
        options,
        &target,
        snapshot.path.as_deref(),
        &snapshot.claude_subagents,
    )?;
    tx.execute(
        "UPDATE sessions SET discovery_state = 'full', source_stamp = ?, parser_version = ? \
         WHERE source = ? AND session_id = ?",
        params![
            snapshot.stamp,
            HYDRATION_PARSER_VERSION,
            options.source,
            options.session_id
        ],
    )?;
    tx.execute(
        "UPDATE session_presences SET discovery_state = 'full' \
         WHERE source = ? AND session_id = ? AND location = 'local'",
        params![options.source, options.session_id],
    )?;
    let last_event_at_ms = max_event_time(&tx, &options.source, &options.session_id)?;
    let last_tool_result_index = max_tool_result_index(&tx, &options.source, &options.session_id)?;
    tx.execute(
        "INSERT INTO session_hydration_checkpoints \
         (source, session_id, location, source_stamp, parser_version, last_event_at_ms, source_bytes, records_parsed, include_related, last_tool_result_index, updated_ms, source_diagnostics_json) \
         VALUES (?, ?, 'local', ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, location) DO UPDATE SET \
           source_stamp = excluded.source_stamp, parser_version = excluded.parser_version, \
           last_event_at_ms = excluded.last_event_at_ms, source_bytes = excluded.source_bytes, \
           records_parsed = excluded.records_parsed, include_related = excluded.include_related, \
           last_tool_result_index = excluded.last_tool_result_index, \
           updated_ms = excluded.updated_ms, \
           source_diagnostics_json = excluded.source_diagnostics_json",
        params![
            options.source,
            options.session_id,
            snapshot.stamp,
            HYDRATION_PARSER_VERSION,
            last_event_at_ms,
            snapshot.bytes,
            records_parsed,
            options.include_related,
            last_tool_result_index,
            now_ms(),
            serde_json::to_string(&source_diagnostics).ok(),
        ],
    )?;
    let local_observation=local_observation.unwrap_or(SessionObservation{key:local_key,raw_locator:target.locator.clone(),source_stamp:tx.query_row("SELECT source_stamp FROM session_presences WHERE source=? AND session_id=? AND location='local'",params![options.source,options.session_id],|row|row.get(0)).optional()?.flatten(),discovery_state:"shallow".into(),access_state:"available".into(),updated_ms:now_ms()});
    save_observation_progress(
        &tx,
        &local_observation,
        &snapshot.stamp,
        snapshot.bytes,
        records_parsed,
        options.include_related,
        true,
    )?;
    // Inside the transaction and after every relationship this hydration
    // recorded: a subagent transcript is routinely read before its parent, so
    // the child's inheritance can only be settled once the whole selected
    // session has landed. Leaving it to the next sync would serve a hydrated
    // session with a null project key in between.
    crate::store::refresh_project_identity(&tx)?;
    tx.commit()?;
    if let (Some(path), Some(consumed)) = (snapshot.path.as_deref(), cursor_consumed_through) {
        // Hydration rebuilt history from offset 0 and does not otherwise
        // move the Cursor byte cursor. A later incremental sync would
        // resume from the old offset and insert untimed prompts again
        // under the current mtime. Checkpoint exactly the bytes this
        // pass indexed: a later append is the next sync's work.
        record_cursor_hydrate_checkpoint(db_path, path, consumed)?;
    }

    let status = if previous_stamp.is_some() {
        "updated"
    } else {
        "hydrated"
    };
    build_result_with(
        &conn,
        options,
        status,
        snapshot.stamp,
        snapshot.bytes,
        records_parsed,
        started.elapsed().as_millis() as i64,
        source_diagnostics,
        snapshot.codex_relationship_complete,
    )
}

struct RemoteHydrationLock {
    file: fs::File,
}

impl Drop for RemoteHydrationLock {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.file);
    }
}

fn acquire_remote_hydration_lock(
    db_path: &Path,
    options: &HydrateSessionOptions,
) -> Result<RemoteHydrationLock> {
    let absolute = if db_path.is_absolute() {
        db_path.to_path_buf()
    } else {
        std::env::current_dir()?.join(db_path)
    };
    let canonical_db = fs::canonicalize(&absolute).or_else(|_| {
        let parent = absolute.parent().unwrap_or_else(|| Path::new("."));
        let parent = fs::canonicalize(parent)?;
        Ok::<_, std::io::Error>(
            parent.join(
                absolute
                    .file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("history.db")),
            ),
        )
    })?;
    let lock_dir = canonical_db
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".relayhistory-hydration-locks");
    fs::create_dir_all(&lock_dir)?;
    let key = format!("{}\0{}", options.source, options.session_id);
    let lock_path = lock_dir.join(format!("{:x}.lock", Sha256::digest(key.as_bytes())));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("opening hydration lock {}", lock_path.display()))?;
    crate::file_lock::lock_exclusive(&file)
        .with_context(|| format!("locking hydration target {}", lock_path.display()))?;
    Ok(RemoteHydrationLock { file })
}

fn hydrate_remote_session(
    _conn: &mut Connection,
    _options: &HydrateSessionOptions,
    _home: &Path,
    _connectors: &crate::remote::SourceConnectorSelection,
    _started: Instant,
) -> Result<HydrateSessionResult> {
    anyhow::bail!(
        "CONNECTOR_NOT_CONFIGURED: install a source plugin and use the composed source registry"
    )
}

fn classify_remote_error(error: anyhow::Error) -> anyhow::Error {
    let message = redact_remote_diagnostic(&format!("{error:#}"));
    for code in [
        "AUTHENTICATION_EXPIRED",
        "SESSION_NOT_FOUND",
        "EVIDENCE_PARTIAL",
        "CONNECTOR_FAILURE",
        "CONNECTOR_NOT_CONFIGURED",
        "INVALID_ARGUMENT",
    ] {
        if message.starts_with(&format!("{code}:")) {
            return anyhow::anyhow!(message);
        }
    }
    hydration_error("CONNECTOR_FAILURE", message)
}

fn redact_remote_diagnostic(message: &str) -> String {
    let mut redacted = Vec::new();
    let parts = message.split_whitespace().collect::<Vec<_>>();
    let mut index = 0;
    while index < parts.len() {
        let part = parts[index];
        let lower = part.to_ascii_lowercase();
        let normalized = lower.trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_');
        let token_key = matches!(
            normalized,
            "access_token" | "accesstoken" | "refresh_token" | "refreshtoken"
        );
        let bearer = normalized == "bearer";
        let authorization_bearer = normalized == "authorization"
            && parts
                .get(index + 1)
                .is_some_and(|next| next.eq_ignore_ascii_case("bearer"));
        let inline_secret = lower.contains("sk-ant-")
            || [
                "access_token",
                "accesstoken",
                "refresh_token",
                "refreshtoken",
            ]
            .iter()
            .any(|key| {
                lower
                    .find(key)
                    .is_some_and(|start| lower[start + key.len()..].starts_with(['=', ':']))
            });
        if inline_secret || probable_secret(part) {
            redacted.push("[REDACTED]");
        } else if authorization_bearer {
            redacted.push("[REDACTED]");
            index += 2;
        } else if bearer || token_key {
            redacted.push("[REDACTED]");
            index += 1;
            if matches!(parts.get(index), Some(&"=") | Some(&":")) {
                index += 1;
            }
        } else {
            redacted.push(part);
        }
        index += 1;
    }
    redacted.join(" ")
}

fn probable_secret(part: &str) -> bool {
    let value =
        part.trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && !matches!(ch, '-' | '_' | '.'));
    value.len() >= 24
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && value.bytes().any(|byte| byte.is_ascii_lowercase())
        && value.bytes().any(|byte| byte.is_ascii_uppercase())
        && value.bytes().any(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
fn remote_limited_result(
    conn: &mut Connection,
    options: &HydrateSessionOptions,
    code: &str,
    message: String,
    started: Instant,
) -> Result<HydrateSessionResult> {
    if options.source == "codex" && code == "PROVIDER_CAPABILITY_LIMITED" {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM file_edits WHERE source = 'codex' AND session_id = ? AND tool_name = 'codex cloud diff'",
            [&options.session_id],
        )?;
        tx.execute(
            "DELETE FROM session_hydration_checkpoints WHERE source = 'codex' AND session_id = ? AND location = 'remote'",
            [&options.session_id],
        )?;
        tx.commit()?;
    }
    let related_session_ids = if options.include_related {
        related_ids(conn, &options.source, &options.session_id)?
    } else {
        Vec::new()
    };
    let mut ids = vec![options.session_id.clone()];
    ids.extend(related_session_ids.iter().cloned());
    let evidence = evidence_counts(
        conn,
        &options.source,
        &ids,
        related_session_ids.len() as u64,
    )?;
    Ok(HydrateSessionResult {
        contract_version: SESSION_HYDRATION_CONTRACT_VERSION,
        source: options.source.clone(),
        session_id: options.session_id.clone(),
        status: "capability_limited".to_string(),
        capability: "shallow_only".to_string(),
        discovery_state: "shallow".to_string(),
        presence: "remote".to_string(),
        indexed_through: HydrationIndexedThrough::default(),
        evidence,
        coverage: Vec::new(),
        related_session_ids,
        diagnostics: vec![HydrationDiagnostic {
            code: code.to_string(),
            message: redact_remote_diagnostic(&message),
            duration_ms: Some(started.elapsed().as_millis() as i64),
            source_bytes: Some(0),
            records_parsed: Some(0),
        }],
    })
}

#[cfg(test)]
fn hydrate_remote_claude(
    conn: &mut Connection,
    options: &HydrateSessionOptions,
    records: Vec<Value>,
    source_stamp: String,
    source_bytes: i64,
    started: Instant,
) -> Result<HydrateSessionResult> {
    hydrate_remote_claude_observed(
        conn,
        options,
        records,
        source_stamp,
        source_bytes,
        started,
        None,
    )
}

#[cfg(test)]
fn hydrate_remote_claude_observed(
    conn: &mut Connection,
    options: &HydrateSessionOptions,
    mut records: Vec<Value>,
    source_stamp: String,
    source_bytes: i64,
    started: Instant,
    observation: Option<&SessionObservation>,
) -> Result<HydrateSessionResult> {
    let previous = match observation {
        Some(observation) => {
            observations::checkpoint(conn, &observation.key)?.map(|value| HydrationCheckpoint {
                source_stamp: value.source_stamp,
                parser_version: value.parser_version,
            })
        }
        None => hydration_checkpoint(conn, options, "remote")?,
    };
    if previous.as_ref().is_some_and(|checkpoint| {
        checkpoint.source_stamp.as_deref() == Some(source_stamp.as_str())
            && checkpoint.parser_version == HYDRATION_PARSER_VERSION
    }) {
        if let Some(observation) = observation {
            observations::set_access(conn, &observation.key, "available")?;
        }
        return build_remote_result(
            conn,
            options,
            "unchanged",
            "full",
            "full",
            crate::source_evidence::FULL_SESSION_KINDS.to_vec(),
            source_stamp,
            source_bytes,
            records.len() as i64,
            "REMOTE_EVIDENCE_FULL",
            "all evidence exposed by Claude's teleport interface is indexed",
            started,
        );
    }
    for record in &mut records {
        let object = record
            .as_object_mut()
            .context("CONNECTOR_FAILURE: Claude teleport evidence contains a non-object record")?;
        let recorded_id = object
            .get("sessionId")
            .or_else(|| object.get("session_id"))
            .and_then(Value::as_str);
        match recorded_id {
            Some(id) if id != options.session_id => anyhow::bail!(
                "CONNECTOR_FAILURE: Claude teleport record identity does not match the requested session"
            ),
            None => {}
            _ => {}
        }
        // The shared transcript parser consumes the provider's canonical
        // camelCase field. Normalize the accepted snake_case wire variant.
        object.insert(
            "sessionId".to_string(),
            Value::String(options.session_id.clone()),
        );
    }
    let transcript = crate::jsonl_temp::JsonlTemp::write(records.iter())?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let had_local_presence: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_presences WHERE source = ? AND session_id = ? AND location = 'local')",
        params![options.source, options.session_id],
        |row| row.get(0),
    )?;
    // A teleport response is a complete remote snapshot. Remove evidence that
    // disappeared before inserting the new snapshot. When the canonical id
    // also has local evidence, do not erase rows whose provenance cannot be
    // distinguished from the remote copy.
    let other_observation = observations::list(&tx, &options.source, &options.session_id)?
        .iter()
        .any(|other| observation.is_some_and(|current| other.key != current.key));
    if !had_local_presence && !other_observation {
        // `session_markers` belongs in this list for the same reason the
        // others do: a marker is evidence, and one left behind after the
        // provider stopped sending the record it describes tells a caller
        // something is still there that is not.
        for table in [
            "history",
            "session_events",
            "tool_calls",
            "file_edits",
            "session_markers",
        ] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE source = 'claude' AND session_id = ?"),
                [&options.session_id],
            )?;
        }
    }
    ingest_claude_transcript(&tx, transcript.path())?;
    if !had_local_presence {
        tx.execute(
            "DELETE FROM session_presences WHERE source = ? AND session_id = ? AND location = 'local'",
            params![options.source, options.session_id],
        )?;
    }
    tx.execute(
        "UPDATE sessions SET discovery_state = 'full' WHERE source = ? AND session_id = ?",
        params![options.source, options.session_id],
    )?;
    tx.execute(
        "UPDATE session_presences SET discovery_state = 'full', source_stamp = ? \
         WHERE source = ? AND session_id = ? AND location = 'remote'",
        params![source_stamp, options.source, options.session_id],
    )?;
    write_hydration_checkpoint(
        &tx,
        options,
        "remote",
        &source_stamp,
        source_bytes,
        records.len() as i64,
    )?;
    if let Some(observation) = observation {
        save_observation_progress(
            &tx,
            observation,
            &source_stamp,
            source_bytes,
            records.len() as i64,
            options.include_related,
            true,
        )?;
        observations::save_evidence(
            &tx,
            &observation.key,
            &json!({"format":"claude","records":records}),
        )?;
    }
    tx.commit()?;
    build_remote_result(
        conn,
        options,
        if previous.is_some() {
            "updated"
        } else {
            "hydrated"
        },
        "full",
        "full",
        crate::source_evidence::FULL_SESSION_KINDS.to_vec(),
        source_stamp,
        source_bytes,
        records.len() as i64,
        "REMOTE_EVIDENCE_FULL",
        "all evidence exposed by Claude's teleport interface is indexed",
        started,
    )
}

#[cfg(test)]
fn hydrate_remote_codex_diff(
    conn: &mut Connection,
    options: &HydrateSessionOptions,
    diff: &str,
    source_stamp: String,
    source_bytes: i64,
    started: Instant,
) -> Result<HydrateSessionResult> {
    hydrate_remote_codex_diff_observed(
        conn,
        options,
        diff,
        source_stamp,
        source_bytes,
        started,
        None,
    )
}

#[cfg(test)]
fn hydrate_remote_codex_diff_observed(
    conn: &mut Connection,
    options: &HydrateSessionOptions,
    diff: &str,
    source_stamp: String,
    source_bytes: i64,
    started: Instant,
    observation: Option<&SessionObservation>,
) -> Result<HydrateSessionResult> {
    let previous = match observation {
        Some(observation) => {
            observations::checkpoint(conn, &observation.key)?.map(|value| HydrationCheckpoint {
                source_stamp: value.source_stamp,
                parser_version: value.parser_version,
            })
        }
        None => hydration_checkpoint(conn, options, "remote")?,
    };
    let unchanged = previous.as_ref().is_some_and(|checkpoint| {
        checkpoint.source_stamp.as_deref() == Some(source_stamp.as_str())
            && checkpoint.parser_version == HYDRATION_PARSER_VERSION
    });
    if !unchanged {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let prefix = observation
            .map(|observation| {
                format!(
                    "remote-diff:{:x}:",
                    Sha256::digest(format!(
                        "{}\0{}\0{}",
                        observation.key.location.as_str(),
                        observation.key.connector_id,
                        observation.key.connector_instance
                    ))
                )
            })
            .unwrap_or_else(|| "remote-diff:".into());
        tx.execute("DELETE FROM file_edits WHERE source='codex' AND session_id=? AND tool_name='codex cloud diff' AND substr(tool_use_id,1,?)=?",params![options.session_id,prefix.len(),prefix])?;
        for (index, patch) in split_unified_diff(diff).into_iter().enumerate() {
            // Old releases materialized the only remote diff under this bare
            // provider key. Its overwritten presence cannot identify an owner.
            // Retain that canonical projection instead of duplicating it under
            // the new connector prefix; fresh bytes still enter own evidence.
            let legacy_id = format!("remote-diff:{index}");
            if observation.is_some() && tx.query_row("SELECT EXISTS(SELECT 1 FROM file_edits WHERE source='codex' AND session_id=? AND tool_name='codex cloud diff' AND tool_use_id=?)",params![options.session_id,legacy_id],|row|row.get::<_,bool>(0))? {
                continue;
            }

            let (added, removed) = count_unified_diff_lines(&patch.text);
            tx.execute(
                "INSERT INTO file_edits \
                 (source, session_id, tool_use_id, file_path, tool_name, lines_added, lines_removed, structured_patch_json, ts_ms) \
                 VALUES ('codex', ?, ?, ?, 'codex cloud diff', ?, ?, ?, ?) \
                 ON CONFLICT(source, session_id, tool_use_id) DO UPDATE SET \
                   file_path=excluded.file_path, lines_added=excluded.lines_added, lines_removed=excluded.lines_removed, \
                   structured_patch_json=excluded.structured_patch_json, ts_ms=excluded.ts_ms",
                params![
                    options.session_id,
                    format!("{prefix}{index}"),
                    patch.path,
                    added,
                    removed,
                    serde_json::to_string(&serde_json::json!({"unified_diff": patch.text}))?,
                    now_ms(),
                ],
            )?;
        }
        write_hydration_checkpoint(&tx, options, "remote", &source_stamp, source_bytes, 1)?;
        if let Some(observation) = observation {
            save_observation_progress(
                &tx,
                observation,
                &source_stamp,
                source_bytes,
                1,
                options.include_related,
                false,
            )?;
            observations::save_evidence(
                &tx,
                &observation.key,
                &json!({"format":"codex-diff","diff":diff}),
            )?;
        }
        tx.commit()?;
    }
    build_remote_result(
        conn,
        options,
        if unchanged {
            "unchanged"
        } else if previous.is_some() {
            "updated"
        } else {
            "hydrated"
        },
        "partial",
        "shallow",
        // The cloud task exposes its diff and nothing else.
        vec![EvidenceKind::FileEdit],
        source_stamp,
        source_bytes,
        1,
        "EVIDENCE_PARTIAL",
        "Codex exposes the task diff but no supported transcript, tool-result, token, model, or agent-relationship export",
        started,
    )
}

struct UnifiedPatch {
    path: String,
    text: String,
}

fn split_unified_diff(diff: &str) -> Vec<UnifiedPatch> {
    let mut patches = Vec::new();
    let mut current_path: Option<String> = None;
    let mut current = String::new();
    for line in diff.lines() {
        if let Some(path) = git_diff_destination_path(line) {
            if let Some(path) = current_path.take() {
                patches.push(UnifiedPatch {
                    path,
                    text: std::mem::take(&mut current),
                });
            }
            current_path = Some(path);
        }
        if current_path.is_some() {
            current.push_str(line);
            current.push('\n');
        }
    }
    if let Some(path) = current_path {
        patches.push(UnifiedPatch {
            path,
            text: current,
        });
    }
    if patches.is_empty() && !diff.trim().is_empty() {
        patches.push(UnifiedPatch {
            path: "(task diff)".to_string(),
            text: diff.to_string(),
        });
    }
    patches
}

fn git_diff_destination_path(line: &str) -> Option<String> {
    let mut rest = line.strip_prefix("diff --git ")?;
    let _source = take_git_path(&mut rest)?;
    let destination = take_git_path(&mut rest)?;
    destination.strip_prefix("b/").map(str::to_string)
}

fn take_git_path(input: &mut &str) -> Option<String> {
    *input = input.trim_start();
    if let Some(quoted) = input.strip_prefix('"') {
        let mut bytes = Vec::new();
        let raw = quoted.as_bytes();
        let mut index = 0;
        while index < raw.len() {
            match raw[index] {
                b'"' => {
                    *input = &quoted[index + 1..];
                    return Some(String::from_utf8_lossy(&bytes).into_owned());
                }
                b'\\' if index + 1 < raw.len() => {
                    index += 1;
                    match raw[index] {
                        b'n' => bytes.push(b'\n'),
                        b'r' => bytes.push(b'\r'),
                        b't' => bytes.push(b'\t'),
                        b'0'..=b'7' => {
                            let mut value = raw[index] - b'0';
                            for _ in 0..2 {
                                if index + 1 < raw.len() && matches!(raw[index + 1], b'0'..=b'7') {
                                    index += 1;
                                    value =
                                        value.saturating_mul(8).saturating_add(raw[index] - b'0');
                                }
                            }
                            bytes.push(value);
                        }
                        escaped => bytes.push(escaped),
                    }
                }
                byte => bytes.push(byte),
            }
            index += 1;
        }
        None
    } else {
        let end = input.find(char::is_whitespace).unwrap_or(input.len());
        let token = input[..end].to_string();
        *input = &input[end..];
        Some(token)
    }
}

#[cfg(test)]
struct HydrationCheckpoint {
    source_stamp: Option<String>,
    parser_version: i64,
}

#[cfg(test)]
fn hydration_checkpoint(
    conn: &Connection,
    options: &HydrateSessionOptions,
    location: &str,
) -> Result<Option<HydrationCheckpoint>> {
    Ok(conn
        .query_row(
            "SELECT source_stamp, parser_version FROM session_hydration_checkpoints WHERE source = ? AND session_id = ? AND location = ?",
            params![options.source, options.session_id, location],
            |row| Ok(HydrationCheckpoint { source_stamp: row.get(0)?, parser_version: row.get(1)? }),
        )
        .optional()?)
}

#[cfg(test)]
fn write_hydration_checkpoint(
    conn: &Connection,
    options: &HydrateSessionOptions,
    location: &str,
    source_stamp: &str,
    source_bytes: i64,
    records_parsed: i64,
) -> Result<()> {
    let last_event_at_ms = max_event_time(conn, &options.source, &options.session_id)?;
    let last_tool_result_index = max_tool_result_index(conn, &options.source, &options.session_id)?;
    conn.execute(
        "INSERT INTO session_hydration_checkpoints \
         (source, session_id, location, source_stamp, parser_version, last_event_at_ms, source_bytes, records_parsed, include_related, last_tool_result_index, updated_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, location) DO UPDATE SET \
           source_stamp=excluded.source_stamp, parser_version=excluded.parser_version, last_event_at_ms=excluded.last_event_at_ms, \
           source_bytes=excluded.source_bytes, records_parsed=excluded.records_parsed, include_related=excluded.include_related, \
           last_tool_result_index=excluded.last_tool_result_index, updated_ms=excluded.updated_ms",
        params![
            options.source,
            options.session_id,
            location,
            source_stamp,
            HYDRATION_PARSER_VERSION,
            last_event_at_ms,
            source_bytes,
            records_parsed,
            options.include_related,
            last_tool_result_index,
            now_ms(),
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_remote_result(
    conn: &Connection,
    options: &HydrateSessionOptions,
    status: &str,
    capability: &str,
    discovery_state: &str,
    coverage: Vec<EvidenceKind>,
    source_stamp: String,
    source_bytes: i64,
    records_parsed: i64,
    diagnostic_code: &str,
    diagnostic_message: &str,
    started: Instant,
) -> Result<HydrateSessionResult> {
    let related_session_ids = if options.include_related {
        related_ids(conn, &options.source, &options.session_id)?
    } else {
        Vec::new()
    };
    let mut ids = vec![options.session_id.clone()];
    ids.extend(related_session_ids.iter().cloned());
    Ok(HydrateSessionResult {
        contract_version: SESSION_HYDRATION_CONTRACT_VERSION,
        source: options.source.clone(),
        session_id: options.session_id.clone(),
        status: status.to_string(),
        capability: capability.to_string(),
        discovery_state: discovery_state.to_string(),
        presence: if options.scope == SessionScope::Local {
            "local"
        } else {
            "remote"
        }
        .to_string(),
        indexed_through: HydrationIndexedThrough {
            source_stamp: Some(source_stamp),
            last_event_at_ms: max_event_time(conn, &options.source, &options.session_id)?,
        },
        evidence: evidence_counts(
            conn,
            &options.source,
            &ids,
            related_session_ids.len() as u64,
        )?,
        related_session_ids,
        diagnostics: std::iter::once(HydrationDiagnostic {
            code: diagnostic_code.to_string(),
            message: diagnostic_message.to_string(),
            duration_ms: Some(started.elapsed().as_millis() as i64),
            source_bytes: Some(source_bytes),
            records_parsed: Some(records_parsed),
        })
        // A remote or plugin snapshot that covers less than a full session
        // names what it left out, exactly as the local path does.
        .chain(partial_coverage_diagnostic(options, &coverage, false))
        .collect(),
        coverage,
    })
}

fn validate_options(options: &HydrateSessionOptions) -> Result<()> {
    if options.session_id.trim().is_empty() {
        return Err(hydration_error(
            "INVALID_ARGUMENT",
            "sessionId must not be empty",
        ));
    }
    if !matches!(
        options.source.as_str(),
        "claude" | "codex" | "cursor" | "grok" | "relay" | "opencode"
    ) {
        return Err(hydration_error(
            "INVALID_ARGUMENT",
            format!("unsupported catalog source '{}'", options.source),
        ));
    }
    Ok(())
}

fn catalog_target(conn: &Connection, options: &HydrateSessionOptions) -> Result<CatalogTarget> {
    let location = if options.scope == SessionScope::Remote {
        "remote"
    } else {
        "local"
    };
    let row = conn
        .query_row(
            "SELECT CASE WHEN s.source='opencode' AND p.location='local' THEN s.raw_path ELSE p.raw_locator END, COALESCE(p.discovery_state, s.discovery_state) \
             FROM sessions s JOIN session_presences p \
               ON p.source = s.source AND p.session_id = s.session_id AND p.location = ? \
             WHERE s.source = ? AND s.session_id = ?",
            params![location, options.source, options.session_id],
            |row| {
                Ok(CatalogTarget {
                    locator: row.get(0)?,
                    discovery_state: row.get(1)?,
                })
            },
        )
        .optional()?;
    row.ok_or_else(|| {
        hydration_error(
            "SESSION_NOT_FOUND",
            "Run discoverSessions() before hydrating this session.",
        )
    })
}

fn source_snapshot(
    options: &HydrateSessionOptions,
    target: &CatalogTarget,
    roots: &crate::ProviderRoots,
) -> Result<SourceSnapshot> {
    if options.source == "relay" {
        return Err(hydration_error(
            "HYDRATION_UNSUPPORTED",
            "Relay catalog evidence has no configured full-evidence connector",
        ));
    }
    if options.source == "opencode" {
        let configured_path = &roots.opencode_db;
        let locator = target.locator.as_deref().ok_or_else(|| {
            hydration_error(
                "SESSION_SOURCE_UNAVAILABLE",
                "OpenCode catalog row has no store provenance; run discoverSessions() again",
            )
        })?;
        let path = PathBuf::from(locator);
        if fs::canonicalize(&path).ok() != fs::canonicalize(configured_path).ok() {
            return Err(hydration_error(
                "SESSION_SOURCE_MISMATCH",
                format!(
                    "OpenCode catalog store {} does not match configured store {}",
                    path.display(),
                    configured_path.display()
                ),
            ));
        }
        if !path.is_file() {
            return Err(hydration_error(
                "SESSION_SOURCE_UNAVAILABLE",
                format!("OpenCode source {} is unavailable", path.display()),
            ));
        }
        let src = Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )?;
        let columns = src
            .prepare("SELECT name FROM pragma_table_info('session')")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<std::collections::BTreeSet<_>>>()?;
        let created = if columns.contains("time_created") {
            "time_created"
        } else {
            "NULL"
        };
        let updated = if columns.contains("time_updated") {
            "time_updated"
        } else {
            "NULL"
        };
        let stamp_sql = format!(
            "SELECT printf('%lld:%lld', COALESCE({created}, 0), \
                COALESCE({updated}, {created}, 0)) FROM session WHERE id = ?"
        );
        let stamp = src
            .query_row(&stamp_sql, [&options.session_id], |row| {
                row.get::<_, String>(0)
            })
            .optional()?
            .ok_or_else(|| {
                hydration_error(
                    "SESSION_SOURCE_UNAVAILABLE",
                    format!("OpenCode session '{}' no longer exists", options.session_id),
                )
            })?;
        return Ok(SourceSnapshot {
            stamp,
            bytes: 0,
            // The ingestion query remains session-keyed. Avoid a second count
            // query here so checkpoint resolution never scans the provider's
            // complete `part` table on older stores missing its usual index.
            records: SnapshotRecords::Counted(0),
            path: Some(path),
            claude_subagents: Vec::new(),
            codex_relationship_complete: true,
        });
    }

    let locator = target.locator.as_deref().ok_or_else(|| {
        hydration_error(
            "SESSION_SOURCE_UNAVAILABLE",
            "catalog row has no local provider locator; run discoverSessions() again",
        )
    })?;
    let path = PathBuf::from(locator);
    if !path.is_file() {
        return Err(hydration_error(
            "SESSION_SOURCE_UNAVAILABLE",
            format!(
                "provider source {} disappeared after discovery",
                path.display()
            ),
        ));
    }
    validate_provider_path(&options.source, &path, roots)?;
    // Grok's source is a directory, not a file. Its inventory is metadata
    // only — the same walk as its change stamp, so the two can never describe
    // different sets of files — and the record count is deferred, because
    // taking it means reading an update stream that is routinely megabytes
    // and a run that decides nothing changed has no use for it.
    let (mut bytes, mut records, mut stamp) = if options.source == "grok" {
        let inventory = grok_source_inventory(&path)?;
        (
            inventory.bytes,
            SnapshotRecords::DeferredGrok(path.clone()),
            inventory.stamp,
        )
    } else {
        (
            path.metadata()?.len() as i64,
            SnapshotRecords::Counted(complete_jsonl_records(&path)?),
            file_stamp(&path)?,
        )
    };
    let mut subagents = Vec::new();
    if options.source == "claude" && options.include_related {
        subagents = claude_subagents(&path, &options.session_id)?;
        for evidence in &subagents {
            stamp.push('|');
            stamp.push_str(&file_stamp(&evidence.path)?);
            bytes += evidence.path.metadata()?.len() as i64;
            records = records.plus(complete_jsonl_records(&evidence.path)?);
            // The metadata sidecar describes the child — its type, model and
            // spawn depth, and the tool use that started it — so a sidecar
            // that arrives or changes on its own is still new evidence.
            let metadata = claude_subagent_meta_path(&evidence.path);
            if metadata.is_file() {
                stamp.push('|');
                stamp.push_str(&file_stamp(&metadata)?);
                bytes += metadata.metadata()?.len() as i64;
            }
        }
    }
    let mut codex_relationship_complete = true;
    if options.source == "codex" && options.include_related {
        codex_relationship_complete = match path.parent() {
            Some(directory) => codex_child_scan_complete(directory)?,
            None => false,
        };
        if !codex_relationship_complete {
            stamp.push_str("|codex-relationships-limited");
        }
        for child in codex_children(&path, &options.session_id)? {
            stamp.push('|');
            stamp.push_str(&file_stamp(&child)?);
            bytes += child.metadata()?.len() as i64;
            records = records.plus(complete_jsonl_records(&child)?);
        }
    }
    Ok(SourceSnapshot {
        stamp,
        bytes,
        records,
        path: Some(path),
        claude_subagents: subagents,
        codex_relationship_complete,
    })
}

fn validate_provider_path(
    source: &str,
    path: &Path,
    provider_roots: &crate::ProviderRoots,
) -> Result<()> {
    let roots = match source {
        "claude" => vec![provider_roots.claude.join("projects")],
        "codex" => vec![
            provider_roots.codex.join("sessions"),
            provider_roots.codex.join("archived_sessions"),
        ],
        "cursor" => vec![provider_roots.home.join(".cursor/projects")],
        "grok" => vec![provider_roots.grok.join("sessions")],
        _ => Vec::new(),
    };
    let canonical = fs::canonicalize(path)?;
    let valid = roots
        .iter()
        .filter_map(|root| fs::canonicalize(root).ok())
        .any(|root| canonical.starts_with(root));
    if !valid {
        return Err(hydration_error(
            "SESSION_SOURCE_MISMATCH",
            format!("catalog locator does not belong to the {source} provider root"),
        ));
    }
    Ok(())
}

// Bytes of file *content* this thread has read, for the tests that assert a
// scan stayed on metadata. Thread-local rather than global so tests running in
// parallel cannot see each other's reads; compiled out entirely outside tests.
#[cfg(test)]
thread_local! {
    pub(crate) static CONTENT_BYTES_READ: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn content_bytes_read() -> u64 {
    CONTENT_BYTES_READ.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_content_bytes_read() {
    CONTENT_BYTES_READ.with(|counter| counter.set(0));
}

#[cfg(test)]
fn note_content_read(path: &Path) {
    let read = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    CONTENT_BYTES_READ.with(|counter| counter.set(counter.get() + read));
}

#[cfg(not(test))]
#[inline]
fn note_content_read(_path: &Path) {}

pub(crate) fn complete_jsonl_records(path: &Path) -> Result<i64> {
    note_content_read(path);
    // The rule lives in one place and both passes call it. Deciding here as
    // well is how the count came to disagree with the parse: it stopped at a
    // final row with no newline without testing whether it parsed, so a
    // transcript whose last record was valid but unterminated reported one
    // fewer record than the parse had just read -- and that figure was
    // checkpointed.
    jsonl::count_records(path)
}

/// How many records the last parse of this session read, from its checkpoint.
///
/// A session that has never been parsed has none, and 0 is then the truthful
/// answer: this run read no records either.
fn stored_records_parsed(conn: &Connection, options: &HydrateSessionOptions) -> Result<i64> {
    Ok(conn
        .query_row(
            "SELECT records_parsed FROM session_hydration_checkpoints \
             WHERE source = ? AND session_id = ? AND location = 'local'",
            params![options.source, options.session_id],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()?
        .flatten()
        .unwrap_or(0))
}

/// The provider diagnostics a previous parse of this session recorded.
///
/// A checkpoint written before they were persisted has none, so a provider
/// that has something true to say about *any* stored session says it from the
/// evidence instead. Reporting nothing would be the worst answer: the caller
/// cannot tell "no caveats" from "caveats not loaded".
fn stored_source_diagnostics(
    conn: &Connection,
    options: &HydrateSessionOptions,
) -> Result<Vec<HydrationDiagnostic>> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT source_diagnostics_json FROM session_hydration_checkpoints \
             WHERE source = ? AND session_id = ? AND location = 'local'",
            params![options.source, options.session_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    if let Some(stored) = stored {
        if let Ok(diagnostics) = serde_json::from_str::<Vec<HydrationDiagnostic>>(&stored) {
            return Ok(diagnostics);
        }
    }
    if options.source == "grok" {
        return Ok(vec![grok_usage_diagnostic(stored_grok_context_tokens(
            conn,
            &options.session_id,
        )?)]);
    }
    Ok(Vec::new())
}

/// The newest context-window snapshot already stored for a Grok session.
fn stored_grok_context_tokens(conn: &Connection, session_id: &str) -> Result<Option<i64>> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT token_json FROM session_events \
             WHERE source = 'grok' AND session_id = ? AND token_json IS NOT NULL \
             ORDER BY ts_ms DESC, id DESC LIMIT 1",
            params![session_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    Ok(stored
        .and_then(|stored| serde_json::from_str::<Value>(&stored).ok())
        .and_then(|token| token.get("context_total_tokens").and_then(Value::as_i64)))
}

/// Index the selected session and hand back whatever the provider's own
/// records could not establish, as diagnostics the caller reports verbatim.
fn ingest_selected(
    conn: &Connection,
    options: &HydrateSessionOptions,
    target: &CatalogTarget,
    path: Option<&Path>,
    claude_subagents: &[ClaudeSubagentEvidence],
) -> Result<(Vec<HydrationDiagnostic>, Option<u64>)> {
    match options.source.as_str() {
        "claude" => ingest_claude(conn, options, path.unwrap(), claude_subagents)
            .map(|()| (Vec::new(), None)),
        "codex" => ingest_codex(conn, options, path.unwrap()).map(|()| (Vec::new(), None)),
        "cursor" => {
            let (diagnostics, consumed) = ingest_cursor(conn, options, target, path.unwrap())?;
            Ok((diagnostics, Some(consumed)))
        }
        "grok" => ingest_grok(conn, options, path.unwrap()).map(|diagnostics| (diagnostics, None)),
        "opencode" => {
            sync_opencode_session(conn, path.unwrap(), &options.session_id)?;
            Ok((Vec::new(), None))
        }
        _ => Err(hydration_error(
            "HYDRATION_UNSUPPORTED",
            format!("{} targeted hydration is unavailable", options.source),
        )),
    }
}

fn ingest_claude(
    conn: &Connection,
    options: &HydrateSessionOptions,
    path: &Path,
    subagents: &[ClaudeSubagentEvidence],
) -> Result<()> {
    let meta = scan_claude_session_file(path)?.ok_or_else(|| {
        hydration_error(
            "SESSION_SOURCE_MISMATCH",
            "Claude transcript has no session identity",
        )
    })?;
    if meta.session_id != options.session_id {
        return Err(hydration_error(
            "SESSION_SOURCE_MISMATCH",
            "Claude transcript identity does not match the catalog row",
        ));
    }
    upsert_session(
        conn,
        &meta.session_id,
        "claude",
        meta.cwd.as_deref(),
        meta.git_branch.as_deref(),
        meta.first_ts,
        meta.last_ts,
        meta.last_assistant_text.as_deref(),
        Some(&path.to_string_lossy()),
    )?;
    ingest_claude_transcript(conn, path)?;
    record_claude_remote_relationship(conn, &meta, options.include_related)?;
    if options.include_related {
        // Bank this transcript's continuity evidence for reconciliation now.
        // A request for this session alone leaves relationship evidence for
        // a later hydration that includes related sessions.
        crate::continuity::capture_claude_transcript(conn, path)?;
        crate::continuity::reconcile(conn, "claude")?;
    }
    // The snapshot already walked and parsed these sidecars to stamp them, so
    // this pass indexes that evidence instead of finding it a second time.
    for evidence in subagents {
        ingest_claude_subagent(conn, &options.session_id, evidence)?;
    }
    Ok(())
}

/// Index one Claude subagent transcript and record what established it.
///
/// With an `agentId` the child is a session in its own right: its events are
/// stored under that id so they stay independently addressable, and the id is
/// kept out of the catalog because a delegated thread is not a human session.
/// Without one this provider version simply does not name the child, so the
/// sidechain assistant output stays on the parent exactly as before and the
/// row records unlinked evidence rather than a fabricated identity.
///
/// Shared with the full sync walk, which meets the same sidecars from the
/// other direction: one sidecar produces the same events and the same
/// `session_relationships` row whichever path reaches it first, and the row
/// is an idempotent upsert so the path that arrives second changes nothing.
pub(crate) fn ingest_claude_subagent(
    conn: &Connection,
    parent_session_id: &str,
    evidence: &ClaudeSubagentEvidence,
) -> Result<()> {
    let locator = evidence.path.to_string_lossy().to_string();
    match evidence.agent_id.as_deref() {
        Some(agent_id) => {
            ingest_claude_transcript_as(conn, &evidence.path, Some(agent_id))?;
            cleanup_subagent_registration(conn, "claude", agent_id)?;
            record_relationship(
                conn,
                &ObservedRelationship {
                    source: "claude",
                    parent_session_id,
                    child_session_id: Some(agent_id),
                    relationship: "delegated",
                    child_agent_type: evidence.agent_type.as_deref(),
                    child_agent_name: evidence.description.as_deref(),
                    child_model: evidence.model.as_deref(),
                    spawn_depth: evidence.spawn_depth,
                    evidence_kind: "claude_subagent_meta",
                    evidence_locator: Some(&locator),
                    evidence_ref: evidence.tool_use_id.as_deref(),
                    child_has_events: session_events_exist(conn, "claude", agent_id)?,
                    spawned_at_ms: evidence.first_ts_ms,
                    ..ObservedRelationship::default()
                },
            )
        }
        None => {
            ingest_claude_transcript_as(conn, &evidence.path, None)?;
            record_relationship(
                conn,
                &ObservedRelationship {
                    source: "claude",
                    parent_session_id,
                    child_session_id: None,
                    relationship: "delegated",
                    child_agent_type: evidence.agent_type.as_deref(),
                    child_agent_name: evidence.description.as_deref(),
                    child_model: evidence.model.as_deref(),
                    spawn_depth: evidence.spawn_depth,
                    evidence_kind: "claude_sidechain_records",
                    evidence_locator: Some(&locator),
                    evidence_ref: evidence.tool_use_id.as_deref(),
                    child_has_events: false,
                    spawned_at_ms: evidence.first_ts_ms,
                    ..ObservedRelationship::default()
                },
            )
        }
    }
}

/// One Claude subagent transcript beside a parent's, with whatever identity
/// and description the provider recorded for it.
#[derive(Debug)]
pub(crate) struct ClaudeSubagentEvidence {
    path: PathBuf,
    /// The in-record `agentId`. `None` for provider versions that do not emit
    /// it.
    agent_id: Option<String>,
    agent_type: Option<String>,
    description: Option<String>,
    model: Option<String>,
    spawn_depth: Option<i64>,
    tool_use_id: Option<String>,
    first_ts_ms: Option<i64>,
}

/// Read one subagent sidecar's delegation evidence.
///
/// `meta` is the scan of this same file, whose `agent_id` is the child's
/// identity as the provider recorded it: the file name embeds the same id,
/// but a name is not evidence, and deriving an identity from it would invent
/// one for provider versions that never recorded any. Everything else the
/// delegation is described by comes from the sibling
/// `agent-<agentId>.meta.json`, or from the transcript's first record when
/// that sidecar does not exist.
pub(crate) fn claude_subagent_evidence(
    path: PathBuf,
    meta: &ClaudeSessionMeta,
) -> ClaudeSubagentEvidence {
    let first = first_claude_record(&path);
    let record_str = |key: &str| {
        first
            .as_ref()
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let sidecar = claude_subagent_meta(&path);
    let meta_str = |key: &str| {
        sidecar
            .as_ref()
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    ClaudeSubagentEvidence {
        agent_id: meta.agent_id.clone(),
        agent_type: meta_str("agentType").or_else(|| record_str("attributionAgent")),
        description: meta_str("description"),
        model: meta_str("model"),
        spawn_depth: sidecar
            .as_ref()
            .and_then(|value| value.get("spawnDepth"))
            .and_then(Value::as_i64),
        tool_use_id: meta_str("toolUseId"),
        first_ts_ms: first.as_ref().and_then(|value| {
            value
                .get("timestamp")
                .and_then(|ts| ts.as_str().and_then(parse_iso_ms).or_else(|| ts.as_i64()))
        }),
        path,
    }
}

/// Every subagent transcript belonging to one parent session.
///
/// `collect_matching_files` is recursive, so this reaches both the flat
/// `agent-*.jsonl` layout and `<parentSessionId>/subagents/agent-*.jsonl`, and
/// returns them sorted by path so ingestion is deterministic.
fn claude_subagents(transcript: &Path, session_id: &str) -> Result<Vec<ClaudeSubagentEvidence>> {
    let Some(directory) = transcript.parent() else {
        return Ok(Vec::new());
    };
    let mut evidence = Vec::new();
    for candidate in collect_matching_files(directory, "agent-", "jsonl")? {
        if candidate == transcript {
            continue;
        }
        // A subagent transcript's records carry the PARENT's sessionId, which
        // is what ties this file to the session being hydrated.
        let Some(meta) = scan_claude_session_file(&candidate).ok().flatten() else {
            continue;
        };
        if meta.session_id != session_id {
            continue;
        }
        evidence.push(claude_subagent_evidence(candidate, &meta));
    }
    Ok(evidence)
}

fn first_claude_record(path: &Path) -> Option<Value> {
    let text = fs::read_to_string(path).ok()?;
    text.lines()
        .find_map(|line| serde_json::from_str::<Value>(line).ok())
}

/// Where the `agent-<agentId>.meta.json` sidecar sits beside a subagent
/// transcript. The provider version that writes one names it after the
/// transcript, so the path is derived rather than searched for.
pub(crate) fn claude_subagent_meta_path(transcript: &Path) -> PathBuf {
    transcript.with_extension("meta.json")
}

/// The `agent-<agentId>.meta.json` sidecar beside a subagent transcript, when
/// the provider version writes one.
fn claude_subagent_meta(transcript: &Path) -> Option<Value> {
    let path = claude_subagent_meta_path(transcript);
    serde_json::from_str(&fs::read_to_string(path).ok()?).ok()
}

fn ingest_codex(conn: &Connection, options: &HydrateSessionOptions, path: &Path) -> Result<()> {
    let meta = read_codex_session_meta(path)?.ok_or_else(|| {
        hydration_error(
            "SESSION_SOURCE_MISMATCH",
            "Codex rollout has no session metadata",
        )
    })?;
    if meta.session_id != options.session_id || meta.is_subagent {
        return Err(hydration_error(
            "SESSION_SOURCE_MISMATCH",
            "Codex rollout identity does not match the selected root session",
        ));
    }
    let outcome = ingest_codex_rollout(conn, path, &meta)?;
    if let Some(first) = outcome.first_ts {
        upsert_session(
            conn,
            &meta.session_id,
            "codex",
            Some(&meta.cwd),
            meta.git_branch.as_deref(),
            first,
            outcome.last_ts.unwrap_or(first),
            outcome.last_assistant_text.as_deref(),
            Some(&path.to_string_lossy()),
        )?;
    }
    if options.include_related {
        // Codex records continuity only when a producer writes explicit
        // fields on `session_meta`; a plain `codex resume` leaves no signal.
        crate::continuity::capture_codex_rollout(conn, path)?;
        crate::continuity::reconcile(conn, "codex")?;
        ingest_codex_children(conn, options, path)?;
    }
    Ok(())
}

/// Rollout files that could hold a child of a session started in `directory`.
///
/// Codex partitions rollouts by the date a session started, so a child spawned
/// minutes later can land in the next day's directory -- scanning only the
/// parent's own directory silently loses it while Codex declares full
/// relationship coverage, which is a complete claim over a child nobody looked
/// for.
///
/// Inspect the parent's date and the next date. A targeted request must have
/// a fixed cost with respect to the age of the parent session. If later date
/// directories exist, `codex_child_scan_complete` prevents this bounded search
/// from claiming complete relationship coverage.
fn codex_child_candidates(directory: &Path) -> Result<Vec<PathBuf>> {
    let Some((from, through)) = codex_child_scan_dates(directory) else {
        return collect_matching_files(directory, "rollout-", "jsonl");
    };
    let mut candidates = Vec::new();
    for root in codex_rollout_roots(directory) {
        for date in [&from, &through] {
            candidates.extend(collect_matching_files(
                &root.join(date),
                "rollout-",
                "jsonl",
            )?);
        }
    }
    candidates.sort();
    candidates.dedup();
    Ok(candidates)
}

fn codex_rollout_roots(directory: &Path) -> Vec<PathBuf> {
    let Some(root) = directory
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
    else {
        return vec![directory.to_path_buf()];
    };
    let Some(codex_home) = root.parent() else {
        return vec![root.to_path_buf()];
    };
    match root.file_name().and_then(|name| name.to_str()) {
        Some("sessions" | "archived_sessions") => vec![
            codex_home.join("sessions"),
            codex_home.join("archived_sessions"),
        ],
        _ => vec![root.to_path_buf()],
    }
}

fn codex_child_scan_dates(directory: &Path) -> Option<(String, String)> {
    let from = date_key(directory)?;
    let day = chrono::NaiveDate::parse_from_str(&from, "%Y/%m/%d").ok()?;
    let through = day.succ_opt()?.format("%Y/%m/%d").to_string();
    Some((from, through))
}

fn codex_child_scan_complete(directory: &Path) -> Result<bool> {
    let Some((_, through)) = codex_child_scan_dates(directory) else {
        return Ok(false);
    };
    for root in codex_rollout_roots(directory) {
        'years: for year in sorted_dirs(&root)?.into_iter().rev() {
            for month in sorted_dirs(&year)?.into_iter().rev() {
                if let Some(day) = sorted_dirs(&month)?.pop() {
                    if date_key(&day).is_none_or(|key| key > through) {
                        return Ok(false);
                    }
                    // This is the newest populated date in this root.
                    break 'years;
                }
            }
        }
    }
    Ok(true)
}

/// `YYYY/MM/DD` for a rollout date directory, as a sortable string.
fn date_key(day: &Path) -> Option<String> {
    let name = |path: &Path| {
        path.file_name()
            .and_then(|value| value.to_str())
            .map(str::to_string)
    };
    let month = day.parent()?;
    let year = month.parent()?;
    Some(format!("{}/{}/{}", name(year)?, name(month)?, name(day)?))
}

fn ingest_codex_children(
    conn: &Connection,
    options: &HydrateSessionOptions,
    root_path: &Path,
) -> Result<()> {
    for candidate in codex_children(root_path, &options.session_id)? {
        let Some(meta) = read_codex_session_meta(&candidate)? else {
            continue;
        };
        ingest_codex_rollout(conn, &candidate, &meta)?;
        cleanup_codex_subagent_history(conn, &meta.session_id)?;
        cleanup_codex_subagent_registration(conn, &meta.session_id)?;
        if let Some(parent_session_id) = meta.parent_session_id.as_deref() {
            record_codex_delegation(conn, parent_session_id, &meta, &candidate)?;
        }
    }
    Ok(())
}

fn codex_children(root_path: &Path, parent_session_id: &str) -> Result<Vec<PathBuf>> {
    let Some(directory) = root_path.parent() else {
        return Ok(Vec::new());
    };
    let mut children_by_parent: HashMap<String, Vec<(String, PathBuf)>> = HashMap::new();
    for candidate in codex_child_candidates(directory)? {
        if candidate == root_path {
            continue;
        }
        let Some(meta) = read_codex_session_meta(&candidate)? else {
            continue;
        };
        if !meta.is_subagent {
            continue;
        }
        let Some(parent) = meta.parent_session_id.as_deref() else {
            continue;
        };
        children_by_parent
            .entry(parent.to_string())
            .or_default()
            .push((meta.session_id, candidate));
    }

    let mut descendants = Vec::new();
    let mut pending = vec![parent_session_id.to_string()];
    let mut visited_sessions = HashSet::from([parent_session_id.to_string()]);
    let mut visited_paths = HashSet::new();
    while let Some(parent) = pending.pop() {
        let Some(children) = children_by_parent.remove(&parent) else {
            continue;
        };
        for (child_session_id, path) in children {
            if child_session_id == parent_session_id {
                continue;
            }
            if !visited_paths.insert(path.clone()) {
                continue;
            }
            descendants.push(path);
            if visited_sessions.insert(child_session_id.clone()) {
                pending.push(child_session_id);
            }
        }
    }
    descendants.sort();
    Ok(descendants)
}

fn ingest_cursor(
    conn: &Connection,
    options: &HydrateSessionOptions,
    _target: &CatalogTarget,
    path: &Path,
) -> Result<(Vec<HydrationDiagnostic>, u64)> {
    let project = path
        .ancestors()
        .find(|ancestor| {
            ancestor
                .parent()
                .and_then(Path::file_name)
                .and_then(|s| s.to_str())
                == Some("projects")
        })
        .and_then(Path::file_name)
        .and_then(|s| s.to_str())
        .map(decode_cursor_project);
    let mtime_ms = file_modified_ms(path).unwrap_or(0);
    // Targeted hydration always re-reads the whole transcript, so it is a
    // rebuild: clear every row a previous read left before writing the new
    // one. Upserting on top is not enough. Cursor evidence is keyed on the
    // record's byte offset, and a rewritten transcript reuses those offsets
    // for different records, so the rows an earlier generation wrote past the
    // new end of the file would survive as tool calls and edits this session
    // never made. `history` cannot be upserted at all, because a prompt's
    // identity includes a timestamp an earlier parser took from the mtime.
    //
    // The read happens inside the caller's transaction, and it propagates its
    // error, so a transcript that has vanished or turned unreadable since the
    // snapshot rolls this delete back rather than committing an empty session.
    clear_cursor_session_evidence(conn, &options.session_id)?;
    let outcome = ingest_cursor_transcript(
        conn,
        path,
        &options.session_id,
        project.as_deref(),
        mtime_ms,
        0,
        // Hydration re-reads the whole file. The Cursor byte checkpoint is
        // advanced after this transaction commits, so a later incremental
        // sync does not re-insert the untimed prompts this pass just wrote.
        u64::MAX,
    )?;
    // Authoritative about both ends of the window, having just read the whole
    // file: an expanding merge would keep a mtime endpoint an earlier parser
    // wrote, which MAX() can never retract.
    upsert_session_rebuilt(
        conn,
        &options.session_id,
        "cursor",
        project.as_deref(),
        None,
        outcome.first_ts_ms.unwrap_or(mtime_ms),
        outcome.last_ts_ms.unwrap_or(mtime_ms),
        outcome.last_assistant_text.as_deref(),
        Some(&path.to_string_lossy()),
    )?;
    Ok((cursor_diagnostics(&outcome), outcome.consumed_through))
}

/// What a Cursor transcript could not establish on its own.
///
/// Both codes describe an absence in the provider's records, not a failure of
/// this run: reporting them is the difference between "Cursor does not write
/// this" and "RelayHistory did not read it".
fn cursor_diagnostics(outcome: &CursorTranscriptOutcome) -> Vec<HydrationDiagnostic> {
    let mut diagnostics = Vec::new();
    if outcome.used_mtime_fallback {
        diagnostics.push(HydrationDiagnostic {
            code: "CURSOR_TIMESTAMP_FROM_MTIME".to_string(),
            message: "cursor records carry no timestamp field; events in turns with no readable \
                      <timestamp> tag are stamped with the transcript file mtime"
                .to_string(),
            duration_ms: None,
            source_bytes: None,
            records_parsed: None,
        });
    }
    if outcome.subagent_calls > 0 {
        diagnostics.push(HydrationDiagnostic {
            code: "CURSOR_SUBAGENT_SPAWN_UNLINKED".to_string(),
            message: format!(
                "cursor recorded {} subagent spawn call(s) but writes no child transcript id; \
                 the delegation is visible as a tool call and the child is not addressable",
                outcome.subagent_calls
            ),
            duration_ms: None,
            source_bytes: None,
            records_parsed: None,
        });
    }
    diagnostics
}

fn ingest_grok(
    conn: &Connection,
    options: &HydrateSessionOptions,
    path: &Path,
) -> Result<Vec<HydrationDiagnostic>> {
    let session = scan_grok_session_file(path)?.ok_or_else(|| {
        hydration_error(
            "SESSION_SOURCE_MISMATCH",
            "Grok source has no session identity",
        )
    })?;
    if session.session_id != options.session_id {
        return Err(hydration_error(
            "SESSION_SOURCE_MISMATCH",
            "Grok source identity does not match the catalog row",
        ));
    }
    let outcome = ingest_grok_session(conn, &session, &path.to_string_lossy())?;
    Ok(grok_diagnostics(&outcome))
}

/// What a Grok session directory could not establish on its own.
///
/// Every code here describes an absence in Grok's records, not a failure of
/// this run. `GROK_USAGE_CONTEXT_PROXY_ONLY` is unconditional because it is
/// true of every Grok session: the harness logs no per-turn billing tokens at
/// all, and a consumer that reads `token_json` has to be told that before it
/// adds the numbers up.
/// The one thing that is true of **every** Grok session, parsed or cached:
/// the harness writes no per-turn billing tokens, so the only token fact
/// stored is a context-window snapshot that can go down as well as up.
fn grok_usage_diagnostic(context_total_tokens: Option<i64>) -> HydrationDiagnostic {
    HydrationDiagnostic {
        code: "GROK_USAGE_CONTEXT_PROXY_ONLY".to_string(),
        message: match context_total_tokens {
            Some(total) => format!(
                "grok records no per-turn input/output tokens; the only token fact is the \
                 updates.jsonl context-window snapshot (latest: {total}), which can decrease \
                 on compaction and is not billing usage"
            ),
            None => "grok records no per-turn input/output tokens, and this session's \
                     updates.jsonl carried no totalTokens snapshot either"
                .to_string(),
        },
        duration_ms: None,
        source_bytes: None,
        records_parsed: None,
    }
}

fn grok_diagnostics(outcome: &GrokIngestOutcome) -> Vec<HydrationDiagnostic> {
    let diagnostic = |code: &str, message: String| HydrationDiagnostic {
        code: code.to_string(),
        message,
        duration_ms: None,
        source_bytes: None,
        records_parsed: None,
    };
    let mut diagnostics = vec![grok_usage_diagnostic(outcome.context_total_tokens)];
    if outcome.missing_updates {
        diagnostics.push(diagnostic(
            "GROK_UPDATES_STREAM_MISSING",
            "grok chat_history.jsonl carries no timestamps and this session has no \
             updates.jsonl at all; event times fall back to summary.json"
                .to_string(),
        ));
    }
    let fallbacks = outcome.turn_start_fallbacks + outcome.inherited_fallbacks;
    if fallbacks > 0 || outcome.session_start_fallbacks > 0 {
        diagnostics.push(diagnostic(
            "GROK_TIMESTAMP_FROM_TURN",
            format!(
                "{fallbacks} record(s) took their turn's recorded start or the preceding \
                 record's time, and {} took the session's created_at, because updates.jsonl \
                 recorded no time of their own",
                outcome.session_start_fallbacks
            ),
        ));
    }
    if outcome.encrypted_reasoning > 0 {
        diagnostics.push(diagnostic(
            "GROK_REASONING_ENCRYPTED",
            format!(
                "{} reasoning record(s) carried only an encrypted trace and no readable \
                 summary; each is recorded as an encrypted_reasoning marker rather than as \
                 thinking text",
                outcome.encrypted_reasoning
            ),
        ));
    }
    if outcome.subagent_calls > 0 {
        diagnostics.push(diagnostic(
            "GROK_SUBAGENT_SPAWN_UNLINKED",
            format!(
                "{} subagent spawn call(s) in the transcript name no child session; the \
                 delegation is visible as a tool call, and only a subagents/ metadata entry \
                 can link it",
                outcome.subagent_calls
            ),
        ));
    }
    if outcome.unlinked_subagents > 0 {
        diagnostics.push(diagnostic(
            "GROK_SUBAGENT_METADATA_UNLINKED",
            format!(
                "{} subagents/ entry(ies) recorded no child session id; the delegation is \
                 stored as unlinked evidence and the child is not independently addressable",
                outcome.unlinked_subagents
            ),
        ));
    }
    if outcome.updates_yielded_no_timing {
        diagnostics.push(diagnostic(
            "GROK_UPDATES_STREAM_UNUSABLE",
            "grok wrote an updates.jsonl for this session but none of it established a turn, \
             a message or a tool call; event times fall back to the transcript and the summary"
                .to_string(),
        ));
    }
    if outcome.unread_update_rows > 0 {
        diagnostics.push(diagnostic(
            "GROK_UPDATES_ROWS_UNREAD",
            format!(
                "{} updates.jsonl row(s) carried a sessionUpdate this parser does not \
                 interpret (plan, hook_execution, retry_state, …) and were skipped",
                outcome.unread_update_rows
            ),
        ));
    }
    diagnostics
}

/// Assemble the result, including the diagnostics the provider's parser
/// produced this run — or, on a read that parsed nothing, the ones a previous
/// parse of the same evidence recorded.
#[allow(clippy::too_many_arguments)]
fn build_result_with(
    conn: &Connection,
    options: &HydrateSessionOptions,
    status: &str,
    source_stamp: String,
    source_bytes: i64,
    records_parsed: i64,
    duration_ms: i64,
    source_diagnostics: Vec<HydrationDiagnostic>,
    codex_relationship_complete: bool,
) -> Result<HydrateSessionResult> {
    let related_session_ids = if options.include_related {
        related_ids(conn, &options.source, &options.session_id)?
    } else {
        Vec::new()
    };
    let mut ids = vec![options.session_id.clone()];
    ids.extend(related_session_ids.iter().cloned());
    let evidence = evidence_counts(
        conn,
        &options.source,
        &ids,
        related_session_ids.len() as u64,
    )?;
    let last_event_at_ms = ids.iter().try_fold(None, |max, id| {
        let value = max_event_time(conn, &options.source, id)?;
        Ok::<_, anyhow::Error>(match (max, value) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (None, value) | (value, None) => value,
        })
    })?;
    let mut diagnostics = vec![HydrationDiagnostic {
        code: "HYDRATION_METRICS".to_string(),
        message: "targeted provider evidence acquisition completed".to_string(),
        duration_ms: Some(duration_ms),
        source_bytes: Some(source_bytes),
        records_parsed: Some(records_parsed),
    }];
    diagnostics.extend(source_diagnostics);
    if options.include_related {
        diagnostics.extend(unlinked_diagnostics(
            conn,
            &options.source,
            &options.session_id,
        )?);
    }
    if options.include_related {
        diagnostics.extend(continuity_diagnostics(
            conn,
            &options.source,
            &options.session_id,
        )?);
    }
    if options.source == "codex" && options.include_related && !codex_relationship_complete {
        diagnostics.push(HydrationDiagnostic {
            code: "HYDRATION_BOUNDED_RELATIONSHIPS".to_string(),
            message: "Codex child search examined the session's date and the next date; newer rollout dates exist, so relationship coverage is incomplete".to_string(),
            duration_ms: None,
            source_bytes: None,
            records_parsed: None,
        });
    }
    // Declared coverage narrowed by what this request actually asked for, not a
    // literal: a provider whose local parser only reads prompts has not
    // produced the other four kinds no matter how cleanly the pass completed,
    // and reporting `full` for it is a well-formed answer computed over
    // nothing.
    let coverage = effective_coverage(options, codex_relationship_complete);
    diagnostics.extend(partial_coverage_diagnostic(
        options,
        &coverage,
        options.source == "codex" && !codex_relationship_complete,
    ));
    Ok(HydrateSessionResult {
        contract_version: SESSION_HYDRATION_CONTRACT_VERSION,
        source: options.source.clone(),
        session_id: options.session_id.clone(),
        status: status.to_string(),
        capability: capability_for(&coverage).to_string(),
        // The catalog row's own value, so the reported state cannot disagree
        // with the one the unchanged short-circuit reads back.
        discovery_state: stored_discovery_state(conn, &options.source, &options.session_id)?,
        presence: "local".to_string(),
        indexed_through: HydrationIndexedThrough {
            source_stamp: Some(source_stamp),
            last_event_at_ms,
        },
        evidence,
        coverage,
        related_session_ids,
        diagnostics,
    })
}

/// The delegated threads whose evidence this hydration also acquired.
///
/// Delegation only, on both the seed and the recursive step. A continuation or
/// a fork is a *different conversation* that this one carried on from, not
/// work this session delegated: counting it here would put a separate
/// session's events into this hydration's evidence totals and hand the caller
/// a `relatedSessionIds` it never asked to acquire. Continuity is read through
/// `getSessionRelationships`'s `continuity` array instead. This mirrors
/// `RelationshipKinds::delegation()` — everything that is not a continuity
/// kind — rather than whitelisting `delegated`, so `materialized_local` keeps
/// being related exactly as before.
/// The evidence kinds this hydration could actually have indexed.
///
/// The provider's declared coverage, narrowed by the request: `include_related:
/// false` asks for the selected thread alone, and the acquisition honours that
/// literally -- Claude subagent sidecars are never walked and Codex child
/// rollouts are never read, so no delegation evidence is examined. Reporting
/// `relationship` as covered there would tell a merger that unexamined
/// delegation was fully indexed, which is the same overstatement as the
/// hard-coded `full` this contract replaced.
fn effective_coverage(
    options: &HydrateSessionOptions,
    codex_relationship_complete: bool,
) -> Vec<EvidenceKind> {
    crate::discover::declared_evidence_kinds(&options.source)
        .iter()
        .copied()
        .filter(|kind| {
            *kind != EvidenceKind::Relationship
                || (options.include_related
                    && (options.source != "codex" || codex_relationship_complete))
        })
        .collect()
}

/// The diagnostic naming the evidence kinds a result does not cover.
///
/// Shared by every path that builds a result, so a plugin snapshot missing a
/// kind says the same thing, in the same words, as a local hydration missing
/// one. Without it a `partial` capability arrived with nothing naming what was
/// absent, which is most of what makes `partial` actionable.
///
/// `None` for complete coverage, and also for *empty* coverage: nothing
/// covered is a capability-limited acquisition, reported as `shallow_only`
/// with its own diagnostic, and calling that "partial coverage" would blur the
/// two.
fn partial_coverage_diagnostic(
    options: &HydrateSessionOptions,
    coverage: &[EvidenceKind],
    incomplete_relationships: bool,
) -> Option<HydrationDiagnostic> {
    let missing = missing_from(coverage);
    if coverage.is_empty() || missing.is_empty() {
        return None;
    }
    // The opt-out explains an absent `relationship`, and only when this source
    // would otherwise have produced one. A prompt-only provider is missing four
    // kinds because its parser never reads them, and blaming the request for
    // that sends the reader looking for an option to change instead of at the
    // provider.
    let declined = !options.include_related
        && missing.contains(&EvidenceKind::Relationship)
        && crate::discover::declared_evidence_kinds(&options.source)
            .contains(&EvidenceKind::Relationship);
    Some(HydrationDiagnostic {
        code: "HYDRATION_PARTIAL_COVERAGE".to_string(),
        message: format!(
            "{} evidence covers {}; this hydration {} {}{}",
            options.source,
            crate::source_evidence::join_kinds(coverage),
            if incomplete_relationships {
                "does not fully cover"
            } else {
                "produces no"
            },
            crate::source_evidence::join_kinds(&missing),
            if declined {
                " (include_related is off, so delegation evidence is not read)"
            } else {
                ""
            },
        ),
        duration_ms: None,
        source_bytes: None,
        records_parsed: None,
    })
}

/// The capability a coverage set entitles a result to claim.
///
/// One rule, so the two sides of the contract cannot drift: `full` when
/// nothing is missing, `shallow_only` when nothing at all was covered --
/// `partial` there would imply some kind was indexed -- and `partial`
/// otherwise. The SDK derives the same expectation from `coverage` and rejects
/// a result that disagrees, so a producer that broke this would surface as a
/// native contract mismatch rather than as a quiet overstatement.
///
/// The empty case is not reachable through the public path today: every source
/// `ingest_selected` supports declares at least `History`, and `relay` (which
/// declares nothing) fails `validate_provider_path` long before here. It is
/// written down because `ShallowSessionProvider::evidence_kinds` defaults to
/// `&[]`, so a new adapter that has not declared its kinds yet would otherwise
/// reach the SDK as `partial` over nothing.
pub(crate) fn capability_for(coverage: &[EvidenceKind]) -> &'static str {
    if missing_from(coverage).is_empty() {
        "full"
    } else if coverage.is_empty() {
        "shallow_only"
    } else {
        "partial"
    }
}

/// The `FULL_SESSION_KINDS` a coverage set leaves out, in canonical order.
fn missing_from(coverage: &[EvidenceKind]) -> Vec<EvidenceKind> {
    crate::source_evidence::FULL_SESSION_KINDS
        .iter()
        .copied()
        .filter(|kind| !coverage.contains(kind))
        .collect()
}

/// The `discovery_state` actually stored on the catalog row.
///
/// `full` here means "indexed through the recorded source stamp", which is a
/// different question from how many evidence kinds the provider can produce —
/// that is `capability`. A row with no value has not been deep-indexed.
fn stored_discovery_state(conn: &Connection, source: &str, session_id: &str) -> Result<String> {
    Ok(conn
        .query_row(
            "SELECT discovery_state FROM sessions WHERE source = ? AND session_id = ?",
            params![source, session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten()
        .unwrap_or_else(|| "shallow".to_string()))
}

fn related_ids(conn: &Connection, source: &str, session_id: &str) -> Result<Vec<String>> {
    Ok(conn
        .prepare(
            "WITH RECURSIVE descendants(child_session_id) AS ( \
               SELECT child_session_id FROM session_relationships \
               WHERE source = ?1 AND parent_session_id = ?2 \
                 AND child_session_id IS NOT NULL AND child_session_id != ?2 \
                 AND relationship NOT IN ('continuation', 'fork', 'resume') \
               UNION \
               SELECT relationship.child_session_id FROM session_relationships relationship \
               JOIN descendants ON relationship.parent_session_id = descendants.child_session_id \
               WHERE relationship.source = ?1 AND relationship.child_session_id IS NOT NULL \
                 AND relationship.child_session_id != ?2 \
                 AND relationship.relationship NOT IN ('continuation', 'fork', 'resume') \
             ) \
             SELECT child_session_id FROM descendants ORDER BY child_session_id",
        )?
        .query_map(params![source, session_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Related evidence this session's provider recorded without naming the child.
///
/// Reported as diagnostics rather than as related session ids: there is no id
/// to hand back, and the caller needs to know the evidence exists and where
/// its events actually live.
fn unlinked_diagnostics(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Vec<HydrationDiagnostic>> {
    Ok(conn
        .prepare(
            "SELECT evidence_locator FROM session_relationships \
             WHERE source = ? AND parent_session_id = ? AND child_session_id IS NULL \
               AND relationship NOT IN ('continuation', 'fork', 'resume') \
             ORDER BY relationship_uid",
        )?
        .query_map(params![source, session_id], |row| {
            row.get::<_, Option<String>>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|locator| HydrationDiagnostic {
            code: "RELATIONSHIP_UNLINKED_CHILD".to_string(),
            message: format!(
                "{source} related evidence at {} has no stable child identity; \
                 its events remain attributed to the parent",
                locator.as_deref().unwrap_or("unknown"),
            ),
            duration_ms: None,
            source_bytes: None,
            records_parsed: None,
        })
        .collect())
}

/// Continuity this transcript points at but nothing has indexed yet.
///
/// Reported rather than guessed: a fork with one branch so far and a
/// continuation whose parent record is not stored anywhere are both real,
/// nameable states, and the caller needs to know the evidence was read and is
/// waiting — not that there was none.
fn continuity_diagnostics(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Vec<HydrationDiagnostic>> {
    Ok(
        crate::continuity::pending_reasons(conn, source, session_id)?
            .into_iter()
            .map(|(locator, reason)| HydrationDiagnostic {
                code: crate::continuity::CONTINUITY_UNRESOLVED.to_string(),
                message: format!(
                    "{source} continuity evidence at {locator} is unresolved: {reason}"
                ),
                duration_ms: None,
                source_bytes: None,
                records_parsed: None,
            })
            .collect(),
    )
}

fn evidence_counts(
    conn: &Connection,
    source: &str,
    session_ids: &[String],
    related_sessions: u64,
) -> Result<HydrationEvidence> {
    let mut evidence = HydrationEvidence {
        related_sessions,
        ..Default::default()
    };
    for session_id in session_ids {
        evidence.prompts += count_table(conn, "history", source, session_id)?;
        evidence.events += count_table(conn, "session_events", source, session_id)?;
        evidence.tool_calls += count_table(conn, "tool_calls", source, session_id)?;
        evidence.file_edits += count_table(conn, "file_edits", source, session_id)?;
    }
    Ok(evidence)
}

fn count_table(conn: &Connection, table: &str, source: &str, session_id: &str) -> Result<u64> {
    // `table` is selected exclusively by the private callers above.
    Ok(conn.query_row(
        &format!("SELECT COUNT(*) FROM {table} WHERE source = ? AND session_id = ?"),
        params![source, session_id],
        |row| row.get::<_, i64>(0),
    )? as u64)
}

fn max_event_time(conn: &Connection, source: &str, session_id: &str) -> Result<Option<i64>> {
    Ok(conn.query_row(
        "SELECT MAX(ts_ms) FROM session_events WHERE source = ? AND session_id = ?",
        params![source, session_id],
        |row| row.get(0),
    )?)
}

/// The highest tool-result `event_index` this session has indexed.
///
/// Persisted on the checkpoint so a parser that resumes mid-transcript can
/// continue the sequence (`ToolResultIndexer::resume_from`) instead of
/// restarting it and colliding with indexes already written. Every parser in
/// this crate currently re-reads its transcript from the start, so today this
/// records where the last full parse ended rather than driving it.
fn max_tool_result_index(conn: &Connection, source: &str, session_id: &str) -> Result<Option<i64>> {
    Ok(conn.query_row(
        "SELECT MAX(event_index) FROM session_events WHERE source = ? AND session_id = ?",
        params![source, session_id],
        |row| row.get(0),
    )?)
}

pub(crate) fn hydrate_with_provider(
    db_path: &Path,
    options: &HydrateSessionOptions,
    provider: &dyn ShallowSessionProvider,
) -> Result<HydrateSessionResult> {
    validate_options(options)?;
    let roots = crate::ProviderRoots::from_env(home_dir());
    // Keep transport response acquisition and replacement in the same critical section.
    let _lock = acquire_remote_hydration_lock(db_path, options)?;
    let started = Instant::now();
    let mut conn = open_db(db_path)?;
    let observation = crate::sources::observed(&conn, provider, &options.session_id)?;
    anyhow::ensure!(
        observation.access_state != "withdrawn",
        "CONNECTOR_NOT_CONFIGURED: selected observation is withdrawn"
    );
    let revision =
        observations::revision(&conn, &observation.key)?.context("missing observation revision")?;
    let evidence = match provider.acquire(&roots.home, &observation) {
        Ok(evidence) => evidence,
        Err(error) => {
            observations::set_access(&conn, &observation.key, "unavailable")?;
            return Err(classify_remote_error(error));
        }
    };
    match evidence {
        crate::sources::AcquiredEvidence::LocalFiles => {
            anyhow::ensure!(
                observation.key.location == SessionLocation::Local
                    && observation.key.connector_id == options.source
                    && observation.key.connector_instance == "default",
                "CONNECTOR_FAILURE: local parser requires its built-in observation identity"
            );
            hydrate_session_at_with_roots_and_connectors(
                db_path,
                options,
                &roots,
                &crate::remote::SourceConnectorSelection::new(Vec::new())?,
            )
        }
        evidence @ (crate::sources::AcquiredEvidence::Events(_)
        | crate::sources::AcquiredEvidence::Normalized(_)
        | crate::sources::AcquiredEvidence::ClaudeFull { .. }
        | crate::sources::AcquiredEvidence::CodexDiff { .. }) => {
            let evidence =
                normalize_source_evidence(&options.source, &options.session_id, evidence)?;
            crate::source_intake::apply_normalized(
                &mut conn,
                &observation.key,
                &revision,
                evidence,
                options.include_related,
                started,
            )
        }
        crate::sources::AcquiredEvidence::CapabilityLimited { code, message } => {
            // A failed or listing-only adapter must not remove another connector's evidence.
            build_remote_result(
                &conn,
                options,
                "capability_limited",
                "shallow_only",
                &observation.discovery_state,
                Vec::new(),
                observation.source_stamp.clone().unwrap_or_default(),
                0,
                0,
                code,
                &message,
                started,
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn save_observation_progress(
    conn: &Connection,
    observation: &SessionObservation,
    stamp: &str,
    bytes: i64,
    records: i64,
    include_related: bool,
    full: bool,
) -> Result<()> {
    let mut observation = observation.clone();
    observation.updated_ms = now_ms();
    observation.access_state = "available".into();
    if full {
        observation.discovery_state = "full".into();
    }
    observations::upsert(conn, &observation)?;
    observations::write_checkpoint(
        conn,
        &observation.key,
        &ObservationCheckpoint {
            source_stamp: Some(stamp.into()),
            parser_version: HYDRATION_PARSER_VERSION,
            last_event_at_ms: max_event_time(
                conn,
                &observation.key.source,
                &observation.key.session_id,
            )?,
            source_bytes: bytes,
            records_parsed: records,
            include_related,
            updated_ms: now_ms(),
        },
    )
}

/// Parse provider wire evidence in an isolated database. No managed catalog,
/// credentials or transports are accessed by this normalization operation.
pub fn normalize_source_evidence(
    source: &str,
    session_id: &str,
    evidence: crate::sources::AcquiredEvidence,
) -> Result<crate::source_intake::NormalizedSourceEvidence> {
    use crate::source_evidence::{self, EvidenceKind, EvidenceRecord, PARSED_SESSION_KINDS};
    use crate::sources::AcquiredEvidence;
    let (source_stamp, source_bytes, covered_kinds, records) = match evidence {
        AcquiredEvidence::Events(evidence) => {
            let records = evidence
                .events
                .into_iter()
                .map(|event| {
                    Ok(EvidenceRecord {
                        kind: EvidenceKind::SessionEvent,
                        payload: serde_json::to_value(event)?
                            .as_object()
                            .context("event object")?
                            .clone(),
                        record_id: None,
                        revision_id: None,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            (
                evidence.source_stamp,
                evidence.source_bytes,
                vec![EvidenceKind::SessionEvent],
                records,
            )
        }
        AcquiredEvidence::ClaudeFull {
            mut records,
            source_stamp,
            source_bytes,
        } => {
            anyhow::ensure!(
                source == "claude",
                "CONNECTOR_FAILURE: Claude evidence returned for another source"
            );
            for record in &mut records {
                let object = record
                    .as_object_mut()
                    .context("CONNECTOR_FAILURE: Claude record must be an object")?;
                let id = object
                    .get("sessionId")
                    .or_else(|| object.get("session_id"))
                    .and_then(Value::as_str);
                anyhow::ensure!(
                    id.is_none_or(|id| id == session_id),
                    "CONNECTOR_FAILURE: Claude record identity does not match requested session"
                );
                object.insert("sessionId".into(), Value::String(session_id.into()));
            }
            let transcript = crate::jsonl_temp::JsonlTemp::write(records.iter())?;
            let conn = Connection::open_in_memory()?;
            crate::init_db(&conn)?;
            ingest_claude_transcript(&conn, transcript.path())?;
            // Projected through PARSED_SESSION_KINDS, not the connector
            // capability list: this is our own parser's output coming back out
            // of a temporary database, so the projection has to name every
            // table it just wrote or the rows are dropped here.
            let records =
                source_evidence::read_session(&conn, source, session_id, PARSED_SESSION_KINDS)?;
            (
                source_stamp,
                source_bytes,
                PARSED_SESSION_KINDS.to_vec(),
                records,
            )
        }
        AcquiredEvidence::CodexDiff {
            diff,
            source_stamp,
            source_bytes,
        } => {
            anyhow::ensure!(
                source == "codex",
                "CONNECTOR_FAILURE: Codex evidence returned for another source"
            );
            let records=split_unified_diff(&diff).into_iter().enumerate().map(|(index,patch)| {
                let (added,removed)=count_unified_diff_lines(&patch.text);
                let payload=json!({"source":source,"session_id":session_id,"tool_use_id":format!("remote-diff:{index}"),"file_path":patch.path,"tool_name":"codex cloud diff","lines_added":added,"lines_removed":removed,"structured_patch_json":json!({"unified_diff":patch.text}).to_string()}).as_object().unwrap().clone();
                EvidenceRecord{kind:EvidenceKind::FileEdit,payload,record_id:None,revision_id:None}
            }).collect();
            (
                source_stamp,
                source_bytes,
                vec![EvidenceKind::FileEdit],
                records,
            )
        }
        AcquiredEvidence::Normalized(evidence) => return Ok(evidence),
        AcquiredEvidence::LocalFiles | AcquiredEvidence::CapabilityLimited { .. } => {
            anyhow::bail!("PROVIDER_CAPABILITY_LIMITED: no portable evidence snapshot")
        }
    };
    let mut normalized = crate::source_intake::NormalizedSourceEvidence {
        source_stamp,
        source_bytes,
        covered_kinds,
        records,
    };
    source_evidence::validate_records(
        &ObservationKey {
            source: source.into(),
            session_id: session_id.into(),
            location: SessionLocation::Remote,
            connector_id: "normalizer".into(),
            connector_instance: "default".into(),
        },
        &normalized.covered_kinds,
        &mut normalized.records,
    )?;
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source_evidence::FULL_SESSION_KINDS;
    use std::io::Write;

    #[test]
    fn remote_diagnostics_redact_separated_inline_and_probable_secrets() {
        let message = "Bearer secret-value access_token=inline refresh_token : next-value ALongMixedSecretValue123456";
        let redacted = redact_remote_diagnostic(message);
        assert_eq!(redacted, "[REDACTED] [REDACTED] [REDACTED] [REDACTED]");
        assert!(!redacted.contains("secret"));
        let invalid = classify_remote_error(anyhow::anyhow!(
            "INVALID_ARGUMENT: remote Claude session id is malformed"
        ));
        assert!(invalid.to_string().starts_with("INVALID_ARGUMENT:"));
        assert_eq!(
            redact_remote_diagnostic("CONNECTOR_FAILURE: authorization failed for provider"),
            "CONNECTOR_FAILURE: authorization failed for provider"
        );
        assert_eq!(
            redact_remote_diagnostic("CONNECTOR_FAILURE: Authorization Bearer secret-value"),
            "CONNECTOR_FAILURE: [REDACTED]"
        );
    }

    #[test]
    fn remote_hydration_file_lock_serializes_the_same_session() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let request = HydrateSessionOptions {
            source: "codex".into(),
            session_id: "task_lock".into(),
            scope: SessionScope::Remote,
            include_related: false,
        };
        let first = acquire_remote_hydration_lock(&db, &request).unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let second = acquire_remote_hydration_lock(&db, &request).unwrap();
            sender.send(()).unwrap();
            drop(second);
        });
        assert!(receiver
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err());
        drop(first);
        receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        handle.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn remote_hydration_file_lock_canonicalizes_database_aliases() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_dir = dir.path().join("real");
        fs::create_dir(&real_dir).unwrap();
        let db = real_dir.join("history.db");
        fs::write(&db, []).unwrap();
        let alias_dir = dir.path().join("alias");
        symlink(&real_dir, &alias_dir).unwrap();
        let alias = alias_dir.join("history.db");
        let request = HydrateSessionOptions {
            source: "codex".into(),
            session_id: "task_alias_lock".into(),
            scope: SessionScope::Remote,
            include_related: false,
        };
        let first = acquire_remote_hydration_lock(&db, &request).unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let second = acquire_remote_hydration_lock(&alias, &request).unwrap();
            sender.send(()).unwrap();
            drop(second);
        });
        assert!(receiver
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err());
        drop(first);
        receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        handle.join().unwrap();
    }

    fn catalog_row(conn: &Connection, source: &str, session_id: &str, path: Option<&Path>) {
        conn.execute(
            "INSERT INTO sessions (source, session_id, raw_path, discovery_state) \
             VALUES (?, ?, ?, 'shallow')",
            params![
                source,
                session_id,
                path.map(|path| path.to_string_lossy().to_string())
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_presences \
             (source, session_id, location, raw_locator, source_stamp, discovery_state) \
             VALUES (?, ?, 'local', ?, 'v2:test-discovery-stamp', 'shallow')",
            params![
                source,
                session_id,
                path.map(|path| path.to_string_lossy().to_string())
            ],
        )
        .unwrap();
    }

    /// One `tool_calls` row as the Grok assertions read it back:
    /// `(tool_use_id, name, target, is_error, ts_ms)`.
    type RecordedToolCall = (String, String, Option<String>, Option<i64>, i64);

    /// Copy a checked-in Grok session fixture to where a real install would
    /// put it, and answer with the transcript inside it.
    fn grok_fixture_home(home: &Path, fixture: &str) -> PathBuf {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/grok")
            .join(fixture);
        copy_tree(&root, home);
        let mut found = Vec::new();
        collect_chat_history(home, &mut found);
        found.sort();
        found.pop().expect("a staged chat_history.jsonl")
    }

    fn copy_tree(from: &Path, to: &Path) {
        for entry in fs::read_dir(from).unwrap().flatten() {
            let target = to.join(entry.file_name());
            if entry.path().is_dir() {
                fs::create_dir_all(&target).unwrap();
                copy_tree(&entry.path(), &target);
            } else {
                fs::create_dir_all(target.parent().unwrap()).unwrap();
                fs::copy(entry.path(), &target).unwrap();
            }
        }
    }

    fn collect_chat_history(dir: &Path, found: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_chat_history(&path, found);
            } else if path.file_name().and_then(|name| name.to_str()) == Some("chat_history.jsonl")
            {
                found.push(path);
            }
        }
    }

    /// The acceptance evidence for #167: real times, tools, results, an edit
    /// and a compaction boundary, from one documented-shape session.
    #[test]
    fn grok_hydration_stamps_prompts_with_the_times_updates_jsonl_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(result.status, "hydrated");

        let conn = open_db(&db).unwrap();
        // Both prompts carry the exact `agentTimestampMs` of their
        // `user_message_chunk`, not `created_at + index` (which would have
        // produced 1789560000000 and 1789560000001).
        let prompts: Vec<(String, i64)> = conn
            .prepare(
                "SELECT prompt, timestamp_ms FROM history \
                 WHERE source = 'grok' AND session_id = 'grok-evt-0001' ORDER BY timestamp_ms",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec![
                (
                    "add a retry to the http client".to_string(),
                    1_789_560_000_000
                ),
                ("now a test".to_string(), 1_789_560_120_000),
            ]
        );
        // The synthetic turn stays out of `history`, as it always has.
        assert!(!prompts
            .iter()
            .any(|(prompt, _)| prompt.contains("context window compacted")));

        // The `<user_query>` envelope is stripped, and the prompt is what the
        // person typed.
        assert!(!prompts[0].0.contains("user_query"));

        // Every prose record, tool call and tool result joined to a time
        // `updates.jsonl` recorded. Three records in this transcript have no
        // counterpart in the ACP stream at all — the system preamble, the
        // injected synthetic turn and the trailing encrypted reasoning — and
        // the fallback they took is reported rather than passed off as
        // recorded.
        let fallback = result
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "GROK_TIMESTAMP_FROM_TURN")
            .expect("the fallback diagnostic");
        assert!(
            fallback.message.starts_with("2 record(s)") && fallback.message.contains("and 1 took"),
            "{}",
            fallback.message
        );
        // …and the context-proxy honesty diagnostic is always present.
        let usage = result
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "GROK_USAGE_CONTEXT_PROXY_ONLY")
            .expect("the usage diagnostic");
        assert!(usage.message.contains("no per-turn input/output tokens"));

        let session: (Option<i64>, Option<i64>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT first_activity_ms, last_activity_ms, last_assistant_text, models_json \
                 FROM sessions WHERE source = 'grok' AND session_id = 'grok-evt-0001'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(session.0, Some(1_789_560_000_000));
        assert_eq!(session.1, Some(1_789_560_138_000));
        assert_eq!(session.2.as_deref(), Some("The test fails; fixing next."));
        assert_eq!(session.3.as_deref(), Some(r#"["grok-4-build"]"#));
    }

    #[test]
    fn grok_hydration_indexes_tools_results_edits_and_a_compaction_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert!(result.evidence.events > 0, "{:?}", result.evidence);

        let conn = open_db(&db).unwrap();
        let calls: Vec<RecordedToolCall> = conn
            .prepare(
                "SELECT tool_use_id, name, target, is_error, ts_ms FROM tool_calls \
                 WHERE source = 'grok' AND session_id = 'grok-evt-0001' ORDER BY ts_ms",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            calls,
            vec![
                (
                    "call_shell_1".to_string(),
                    "Shell".to_string(),
                    Some("rg -n retry src/http.ts".to_string()),
                    None,
                    1_789_560_007_000,
                ),
                (
                    "call_edit_1".to_string(),
                    "StrReplace".to_string(),
                    Some("/tmp/demo/src/http.ts".to_string()),
                    None,
                    1_789_560_014_000,
                ),
                (
                    "call_write_1".to_string(),
                    "Write".to_string(),
                    Some("/tmp/demo/test/http.test.ts".to_string()),
                    None,
                    1_789_560_129_000,
                ),
                (
                    // `is_error` comes from the result record *and* the
                    // terminal `status: failed` on the ACP update.
                    "call_shell_2".to_string(),
                    "Shell".to_string(),
                    Some("npm test".to_string()),
                    Some(1),
                    1_789_560_133_000,
                ),
            ]
        );

        // Every call has a `tool_result` event paired to it by call id, timed
        // from the call's own `tool_call_update`.
        let results: Vec<(String, i64)> = conn
            .prepare(
                "SELECT event_uid, ts_ms FROM session_events \
                 WHERE source = 'grok' AND session_id = 'grok-evt-0001' \
                   AND kind = 'tool_result' ORDER BY ts_ms",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            results,
            vec![
                ("result:call_shell_1".to_string(), 1_789_560_011_000),
                ("result:call_edit_1".to_string(), 1_789_560_019_000),
                ("result:call_write_1".to_string(), 1_789_560_131_000),
                ("result:call_shell_2".to_string(), 1_789_560_135_000),
            ]
        );

        let edits: Vec<(String, String)> = conn
            .prepare(
                "SELECT tool_name, file_path FROM file_edits \
                 WHERE source = 'grok' AND session_id = 'grok-evt-0001' ORDER BY ts_ms",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            edits,
            vec![
                (
                    "StrReplace".to_string(),
                    "/tmp/demo/src/http.ts".to_string()
                ),
                (
                    "Write".to_string(),
                    "/tmp/demo/test/http.test.ts".to_string()
                ),
            ]
        );

        // The reasoning summary is thinking; the encrypted-only trace is not.
        let thinking: Vec<String> = conn
            .prepare(
                "SELECT text FROM session_events WHERE source = 'grok' \
                   AND session_id = 'grok-evt-0001' AND kind = 'thinking'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            thinking,
            vec!["Read the client first, then add a bounded retry around the fetch call."]
        );

        let markers = crate::session_markers(&conn, "grok", "grok-evt-0001").unwrap();
        let kinds: Vec<&str> = markers.iter().map(|marker| marker.kind.as_str()).collect();
        assert!(kinds.contains(&"compaction_boundary"), "{kinds:?}");
        assert!(kinds.contains(&"system"), "{kinds:?}");
        assert!(kinds.contains(&"synthetic_turn"), "{kinds:?}");
        assert!(kinds.contains(&"encrypted_reasoning"), "{kinds:?}");
        assert!(kinds.contains(&"prompt_context"), "{kinds:?}");
        let compaction = markers
            .iter()
            .find(|marker| marker.kind == "compaction_boundary")
            .unwrap();
        assert_eq!(compaction.ts_ms, Some(1_789_560_090_000));
        let signals = markers
            .iter()
            .find(|marker| marker.kind == "signals")
            .unwrap();
        assert_eq!(
            signals.text.as_deref(),
            Some("turns=2 compactions=1 context_tokens_used=9210")
        );

        // The context-window proxy is recorded per turn, named as a proxy, on
        // the turn's last assistant message — and it legitimately falls after
        // the compaction.
        let tokens: Vec<String> = conn
            .prepare(
                "SELECT token_json FROM session_events WHERE source = 'grok' \
                   AND session_id = 'grok-evt-0001' AND token_json IS NOT NULL ORDER BY ts_ms",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            tokens,
            vec![
                r#"{"context_total_tokens":18432,"source":"updates.jsonl"}"#,
                r#"{"context_total_tokens":9210,"source":"updates.jsonl"}"#,
            ]
        );

        // The subagents/ entry names its child, so the delegation is linked.
        let relationship: (Option<String>, String, String, Option<String>) = conn
            .query_row(
                "SELECT child_session_id, identity_status, evidence_kind, child_agent_type \
                 FROM session_relationships WHERE source = 'grok' \
                   AND parent_session_id = 'grok-evt-0001'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(relationship.0.as_deref(), Some("grok-evt-0001-review"));
        assert_eq!(relationship.1, "observed");
        assert_eq!(relationship.2, "grok_subagent_dir");
        assert_eq!(relationship.3.as_deref(), Some("reviewer"));

        // Re-hydrating the unchanged directory duplicates nothing.
        let again =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(again.status, "unchanged");
        let counts: (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT (SELECT COUNT(*) FROM session_events WHERE source = 'grok'), \
                        (SELECT COUNT(*) FROM tool_calls WHERE source = 'grok'), \
                        (SELECT COUNT(*) FROM file_edits WHERE source = 'grok'), \
                        (SELECT COUNT(*) FROM history WHERE source = 'grok')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(counts.1, 4);
        assert_eq!(counts.2, 2);
        assert_eq!(counts.3, 2);
        assert_eq!(counts.0 as u64, result.evidence.events);
    }

    /// The older layout #192 captured: per-record timestamps, `tool_use`
    /// blocks inside `content`, and an `updates.jsonl` that is not an ACP
    /// stream. Nothing about it may fall back to `created_at + index` either.
    #[test]
    fn grok_hydration_reads_the_record_timestamped_layout_without_an_acp_stream() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "full-session");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-00000001", Some(&chat));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("grok", "grok-00000001"), dir.path())
                .unwrap();
        let conn = open_db(&db).unwrap();
        let prompts: Vec<(String, i64)> = conn
            .prepare(
                "SELECT prompt, timestamp_ms FROM history WHERE source = 'grok' \
                 ORDER BY timestamp_ms",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec![
                ("add a retry to the client".to_string(), 1_776_643_200_000),
                ("looks good, now the test".to_string(), 1_776_643_320_000),
            ]
        );
        // A `tool_use` block inside `content` is still a tool call, and its
        // file is still an edit.
        let calls: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_calls WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(calls, 1);
        let edit: String = conn
            .query_row(
                "SELECT file_path FROM file_edits WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(edit, "/tmp/project/src/client.ts");
        // This layout's `updates.jsonl` is not an ACP stream, so it reports
        // the absence instead of pretending the rows were read.
        assert!(
            result
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "GROK_UPDATES_ROWS_UNREAD"),
            "{:?}",
            result.diagnostics
        );
        // `usage.input_tokens` in that layout is unverified invention, so no
        // token fact is recorded from it.
        let tokens: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events \
                 WHERE source = 'grok' AND token_json IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tokens, 0);
    }

    /// A session with no `updates.jsonl` and no per-record timestamps: the
    /// prompts still may not be spread one millisecond apart.
    #[test]
    fn grok_hydration_without_any_recorded_time_reports_it_rather_than_inventing_one() {
        let dir = tempfile::tempdir().unwrap();
        let session_dir = dir
            .path()
            .join(".grok/sessions/%2Ftmp%2Fbare/grok-bare-0001");
        fs::create_dir_all(&session_dir).unwrap();
        let chat = session_dir.join("chat_history.jsonl");
        fs::write(
            &chat,
            "{\"type\":\"user\",\"content\":\"first\"}\n{\"type\":\"user\",\"content\":\"second\"}\n",
        )
        .unwrap();
        fs::write(
            session_dir.join("summary.json"),
            r#"{"info":{"id":"grok-bare-0001","cwd":"/tmp/bare"},"created_at":"2026-09-16T12:00:00.000Z"}"#,
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-bare-0001", Some(&chat));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("grok", "grok-bare-0001"), dir.path())
                .unwrap();
        assert!(
            result
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "GROK_UPDATES_STREAM_MISSING"),
            "{:?}",
            result.diagnostics
        );
        let conn = open_db(&db).unwrap();
        let stamps: Vec<i64> = conn
            .prepare("SELECT ts_ms FROM session_events WHERE source = 'grok' ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        // Both events carry the one time Grok actually recorded. The old
        // parser would have written 1789560000000 and 1789560000001 — an
        // ordering it had no evidence for.
        assert_eq!(stamps, vec![1_789_560_000_000, 1_789_560_000_000]);
        // Two prompts one millisecond apart used to be two `history` rows;
        // sharing a timestamp, they are still two rows, because the prompt
        // text is part of the key.
        let prompts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prompts, 2);
    }

    /// Grok **rewrites** `chat_history.jsonl` in place, so a read is a
    /// replacement snapshot and not an append. Evidence whose source record is
    /// gone from the rewritten directory has to go with it.
    ///
    /// The positive control is in the same assertions: the evidence that is
    /// still in the files must still be in the database, so the test cannot
    /// pass by deleting everything.
    #[test]
    fn grok_re_ingestion_drops_the_evidence_the_rewritten_directory_no_longer_has() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let session_dir = chat.parent().unwrap().to_path_buf();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        assert!(grok_has_tool_call(&conn, "call_shell_2"));
        assert!(grok_has_event(&conn, "tool:call_write_1"));
        assert!(grok_has_file_edit(&conn, "/tmp/demo/test/http.test.ts"));
        assert_eq!(grok_marker_count(&conn, "compaction_boundary"), 1);
        assert_eq!(grok_relationship_count(&conn), 1);

        // Grok rebuilds the transcript: the failing `npm test` call and the
        // `Write` that preceded it are gone, the checkpoint file was pruned,
        // and so was the subagent entry. The first prompt and its Shell call
        // survive, and every record has shifted position in the file.
        let rewritten = fs::read_to_string(&chat)
            .unwrap()
            .lines()
            .filter(|line| {
                !line.contains("call_write_1")
                    && !line.contains("call_shell_2")
                    && !line.contains("now a test")
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        fs::write(&chat, rewritten).unwrap();
        fs::remove_file(session_dir.join("compaction_checkpoints/1789560090000.json")).unwrap();
        fs::remove_file(session_dir.join("subagents/agent-review.json")).unwrap();

        let again =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(again.status, "updated");

        // Gone from the files, gone from the evidence.
        assert!(!grok_has_tool_call(&conn, "call_shell_2"));
        assert!(!grok_has_tool_call(&conn, "call_write_1"));
        assert!(!grok_has_event(&conn, "tool:call_write_1"));
        assert!(!grok_has_event(&conn, "result:call_shell_2"));
        assert!(!grok_has_file_edit(&conn, "/tmp/demo/test/http.test.ts"));
        assert_eq!(grok_marker_count(&conn, "compaction_boundary"), 0);
        assert_eq!(grok_relationship_count(&conn), 0);
        let prompts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prompts, 1);

        // Still in the files, still in the evidence — including the rows whose
        // positional `r{idx}` identity moved when the file was rewritten.
        assert!(grok_has_tool_call(&conn, "call_shell_1"));
        assert!(grok_has_tool_call(&conn, "call_edit_1"));
        assert!(grok_has_event(&conn, "result:call_edit_1"));
        assert!(grok_has_file_edit(&conn, "/tmp/demo/src/http.ts"));
        assert_eq!(grok_marker_count(&conn, "system"), 1);
        assert_eq!(grok_marker_count(&conn, "prompt_context"), 1);
        // One system marker, not two: the rewrite moved it from record 0 to
        // record 0, but an `r{idx}` that shifted would otherwise duplicate.
        let markers: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_markers WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            markers, 5,
            "system, synthetic_turn, encrypted_reasoning, prompt_context, signals"
        );
    }

    /// A Grok turn whose whole answer is tool calls — no prose at all — is an
    /// ordinary turn, and its context snapshot has to land somewhere. When the
    /// snapshot was only ever attached to a prose event, that turn's
    /// `totalTokens` was silently dropped.
    #[test]
    fn a_turn_answered_only_with_tool_calls_still_records_its_context_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let session_dir = dir
            .path()
            .join(".grok/sessions/%2Ftmp%2Ftools/grok-tools-0001");
        fs::create_dir_all(&session_dir).unwrap();
        let chat = session_dir.join("chat_history.jsonl");
        fs::write(
            &chat,
            concat!(
                r#"{"type":"user","content":"<user_query>run the tests</user_query>"}"#,
                "\n",
                r#"{"type":"assistant","content":"","model_id":"grok-4-build","tool_calls":[{"id":"call_only_1","name":"Shell","arguments":{"command":"npm test"}}]}"#,
                "\n",
                r#"{"type":"tool_result","tool_call_id":"call_only_1","content":"all green"}"#,
                "\n"
            ),
        )
        .unwrap();
        fs::write(
            session_dir.join("summary.json"),
            br#"{"info":{"id":"grok-tools-0001","cwd":"/tmp/tools"},"created_at":"2026-09-16T12:00:00.000Z"}"#,
        )
        .unwrap();
        fs::write(
            session_dir.join("updates.jsonl"),
            concat!(
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"eventId":"u1","agentTimestampMs":1789560000000,"turnStartMs":1789560000000}}}"#,
                "\n",
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"call_only_1"},"_meta":{"agentTimestampMs":1789560002000,"turnStartMs":1789560000000}}}"#,
                "\n",
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"call_only_1","status":"completed"},"_meta":{"agentTimestampMs":1789560004000,"turnStartMs":1789560000000}}}"#,
                "\n",
                r#"{"method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"turn_completed","totalTokens":4242},"_meta":{"agentTimestampMs":1789560005000,"turnStartMs":1789560000000}}}"#,
                "\n"
            ),
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-tools-0001", Some(&chat));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("grok", "grok-tools-0001"), dir.path())
                .unwrap();
        let conn = open_db(&db).unwrap();
        let recorded: Vec<(String, String)> = conn
            .prepare(
                "SELECT event_uid, token_json FROM session_events \
                 WHERE source = 'grok' AND token_json IS NOT NULL",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            recorded,
            vec![(
                "tool:call_only_1".to_string(),
                r#"{"context_total_tokens":4242,"source":"updates.jsonl"}"#.to_string(),
            )],
            "the turn's only assistant event is its tool use"
        );
        // Positive control: the caveat quotes the same number, so the snapshot
        // was really read and not defaulted.
        assert!(
            result
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("latest: 4242")),
            "{:?}",
            result.diagnostics
        );
    }

    /// Write a minimal Grok session directory and answer with its transcript.
    fn grok_session_dir(
        home: &Path,
        session_id: &str,
        prompt: &str,
        subagent: Option<&str>,
    ) -> PathBuf {
        let dir = home.join(".grok/sessions/%2Ftmp%2Ftree").join(session_id);
        fs::create_dir_all(&dir).unwrap();
        let chat = dir.join("chat_history.jsonl");
        fs::write(
            &chat,
            format!("{{\"type\":\"user\",\"content\":\"{prompt}\"}}\n"),
        )
        .unwrap();
        fs::write(
            dir.join("summary.json"),
            format!(
                r#"{{"info":{{"id":"{session_id}","cwd":"/tmp/tree"}},"created_at":"2026-09-16T12:00:00.000Z"}}"#
            ),
        )
        .unwrap();
        if let Some(child) = subagent {
            fs::create_dir_all(dir.join("subagents")).unwrap();
            fs::write(
                dir.join("subagents/child.json"),
                format!(r#"{{"session_id":"{child}","agent_type":"reviewer"}}"#),
            )
            .unwrap();
        }
        chat
    }

    fn child_has_events(conn: &Connection, parent: &str) -> bool {
        conn.query_row(
            "SELECT child_has_events FROM session_relationships \
             WHERE source = 'grok' AND parent_session_id = ?",
            params![parent],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn tree_child_has_events(conn: &Connection, parent: &str) -> bool {
        let tree = crate::relationships::session_tree(
            conn,
            "grok",
            parent,
            &crate::relationships::SessionTreeOptions::default(),
        )
        .unwrap();
        tree.nodes
            .iter()
            .find(|node| node.depth == 1)
            .expect("the child node")
            .has_events
    }

    /// `session_tree` reads the stored `child_has_events` rather than probing,
    /// so recording it as a constant renders an indexed child as an empty
    /// node. It has to be true of the child, in whichever order the two
    /// sessions are read.
    #[test]
    fn a_grok_child_that_has_events_is_recorded_as_having_them() {
        let dir = tempfile::tempdir().unwrap();
        let child = grok_session_dir(dir.path(), "grok-b", "child work", None);
        let parent = grok_session_dir(dir.path(), "grok-a", "parent work", Some("grok-b"));
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-b", Some(&child));
        catalog_row(&conn, "grok", "grok-a", Some(&parent));
        drop(conn);

        // Child first: the parent's read can see it already has events.
        hydrate_session_at_with_home(&db, &options("grok", "grok-b"), dir.path()).unwrap();
        hydrate_session_at_with_home(&db, &options("grok", "grok-a"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        assert!(child_has_events(&conn, "grok-a"));
        assert!(tree_child_has_events(&conn, "grok-a"));
    }

    /// The other order, which is the one sync produces about half the time:
    /// the parent is read before the child exists in the index at all.
    #[test]
    fn indexing_a_grok_child_later_tells_its_parent_it_is_addressable() {
        let dir = tempfile::tempdir().unwrap();
        let child = grok_session_dir(dir.path(), "grok-b", "child work", None);
        let parent = grok_session_dir(dir.path(), "grok-a", "parent work", Some("grok-b"));
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-b", Some(&child));
        catalog_row(&conn, "grok", "grok-a", Some(&parent));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("grok", "grok-a"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        // The positive control: before the child is indexed the answer is
        // honestly `false`, so the assertion below is not vacuous.
        assert!(!child_has_events(&conn, "grok-a"));
        assert!(!tree_child_has_events(&conn, "grok-a"));

        hydrate_session_at_with_home(&db, &options("grok", "grok-b"), dir.path()).unwrap();
        assert!(child_has_events(&conn, "grok-a"));
        assert!(tree_child_has_events(&conn, "grok-a"));
    }

    /// A marker is evidence, so a marker has to be deliverable.
    ///
    /// `session_markers` is where a Grok session's compaction boundaries,
    /// system lines, synthetic turns and encrypted-reasoning traces are
    /// stored -- for some sessions it is the *only* place anything is stored.
    /// Durable delivery captures a table only if it is in
    /// `delivery::schema::TABLES`, and the new table was not, so an export of
    /// a Grok session carried its events and relationships and silently
    /// dropped every marker. Nothing failed; the export was simply missing
    /// evidence, which is the worst shape this repository's failures take.
    ///
    /// The entry is **appended**, never inserted: `delivery_jobs.bootstrap_kind`
    /// is a persisted index into `TABLES`, so putting a row anywhere but the
    /// end would silently re-point every in-flight job's bootstrap cursor at a
    /// different table.
    #[cfg(feature = "delivery")]
    #[test]
    fn a_hydrated_grok_marker_reaches_a_delivery_export() {
        use crate::delivery::{
            acknowledge, claim_batch, create_job, prepare_batch, store_prepared_payload,
            AcceptanceLevel, DeliveryAcknowledgment, DeliveryJobConfig, DeliveryLimits,
            ExportSelection,
        };

        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);

        let hydrated =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(hydrated.status, "hydrated");

        let conn = open_db(&db).unwrap();
        let markers: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_markers WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(markers > 0, "the fixture has to write markers to test this");

        let job = create_job(
            &conn,
            &DeliveryJobConfig {
                destination_id: "fixture".into(),
                instance_id: "grok".into(),
                account_id: "account".into(),
                mapping_version: "1".into(),
                selection: ExportSelection {
                    all_sources: true,
                    kinds: vec!["session_marker".into(), "session_event".into()],
                    ..ExportSelection::default()
                },
                limits: DeliveryLimits::default(),
            },
            0,
        )
        .unwrap();

        let mut kinds = Vec::new();
        for step in 0..200 {
            let now = step * 10;
            let prepared = prepare_batch(&conn, &job.job_id, now).unwrap();
            if prepared.batch_id.is_none() {
                if prepared.bootstrap_complete && prepared.scanned_records == 0 {
                    break;
                }
                continue;
            }
            let claim = claim_batch(&conn, &job.job_id, "worker", 1000, &|| now)
                .unwrap()
                .expect("a prepared batch is claimable");
            store_prepared_payload(
                &conn,
                &claim.lease,
                &claim.batch.mapping_version,
                "application/json",
                &serde_json::to_string(&claim.batch).unwrap(),
                &|| now,
            )
            .unwrap();
            acknowledge(
                &conn,
                &claim.lease,
                &DeliveryAcknowledgment {
                    batch_id: claim.batch.batch_id.clone(),
                    accepted_revision_ids: claim
                        .batch
                        .records
                        .iter()
                        .map(|record| record.revision_id.clone())
                        .collect(),
                    unsupported_revision_ids: vec![],
                    acceptance_level: AcceptanceLevel::Durable,
                },
                &|| now,
            )
            .unwrap();
            kinds.extend(claim.batch.records.into_iter().map(|record| record.kind));
        }

        let delivered = |kind: &str| kinds.iter().filter(|seen| *seen == kind).count();
        // The positive control, which passed before the fix and still does:
        // the session's events are exported.
        assert!(
            delivered("session_event") > 0,
            "the export carried no events at all, so it proves nothing: {kinds:?}"
        );
        assert_eq!(
            delivered("session_marker") as i64,
            markers,
            "every stored marker has to reach the export: {kinds:?}"
        );
    }

    /// A record the parse reads is a record the count counts.
    ///
    /// The two are separate passes over the same files, and they disagreed:
    /// the parse reads a final record that has no trailing newline (round
    /// seven's rule -- not every writer terminates its last line), while the
    /// count stopped at the missing newline without testing whether it parsed.
    /// A one-line `chat_history.jsonl` with no final newline therefore
    /// produced a user event and a `records_parsed` of **zero**, and that
    /// figure was written into the hydration checkpoint as the session's
    /// settled record total. Both passes now go through `jsonl::classify`.
    #[test]
    fn a_valid_final_record_without_a_newline_is_counted_as_well_as_read() {
        let hydrate_once = |contents: &str| -> (u64, i64) {
            let dir = tempfile::tempdir().unwrap();
            let session = dir
                .path()
                .join(".grok/sessions/%2Ftmp%2Ftail/grok-tail-0001");
            fs::create_dir_all(&session).unwrap();
            let chat = session.join("chat_history.jsonl");
            fs::write(&chat, contents).unwrap();
            fs::write(
                session.join("summary.json"),
                r#"{"info":{"id":"grok-tail-0001","cwd":"/tmp/tail"},"created_at":"2026-01-01T00:00:00.000Z"}"#,
            )
            .unwrap();
            let db = dir.path().join("history.db");
            let conn = open_db(&db).unwrap();
            catalog_row(&conn, "grok", "grok-tail-0001", Some(&chat));
            drop(conn);
            let result =
                hydrate_session_at_with_home(&db, &options("grok", "grok-tail-0001"), dir.path())
                    .unwrap();
            let counted = result
                .diagnostics
                .iter()
                .find(|diagnostic| diagnostic.code == "HYDRATION_METRICS")
                .and_then(|diagnostic| diagnostic.records_parsed)
                .expect("the metrics diagnostic carries the record count");
            (result.evidence.events, counted)
        };

        let record = r#"{"type":"user","content":"only turn"}"#;

        // The baseline: the same record, terminated. Both passes already
        // agreed here, which is why the disagreement stayed hidden. (The count
        // covers the whole directory, so it also includes `summary.json`;
        // these assertions are about the difference between the shapes, not
        // about that total.)
        let (terminated_events, terminated_count) = hydrate_once(&format!("{record}\n"));
        assert_eq!(terminated_events, 1);
        assert!(terminated_count >= 1);

        // The case that was wrong: the very same record, with no final
        // newline. The parse reads it, so the count must count it.
        let (events, counted) = hydrate_once(record);
        assert_eq!(events, terminated_events, "the parse reads it either way");
        assert_eq!(
            counted, terminated_count,
            "a missing final newline does not change how many records there are"
        );

        // The positive control: an unterminated tail that does *not* parse is
        // a record still being written -- read by neither pass and counted by
        // neither. Without this the fix could simply be "count every line".
        let (events, counted) = hydrate_once(&format!("{record}\n{{\"type\":\"us"));
        assert_eq!(
            events, terminated_events,
            "the finished record is still read"
        );
        assert_eq!(
            counted, terminated_count,
            "and the half-written one is still not counted"
        );
    }

    /// The numbers a hydration reports have to describe the read it did. Grok
    /// reads a directory, and its update stream is routinely the largest file
    /// in it.
    #[test]
    fn grok_hydration_metrics_cover_every_file_the_read_consumes() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let session_dir = chat.parent().unwrap().to_path_buf();
        let updates = session_dir.join("updates.jsonl");
        let transcript_bytes = fs::metadata(&chat).unwrap().len() as i64;
        // A stream far larger than the transcript, as a busy session has.
        let padded = fs::read_to_string(&updates).unwrap().repeat(64);
        fs::write(&updates, &padded).unwrap();
        let stream_bytes = fs::metadata(&updates).unwrap().len() as i64;
        assert!(
            stream_bytes > transcript_bytes * 8,
            "the fixture must make the stream dominate: {stream_bytes} vs {transcript_bytes}"
        );

        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        let metrics = result
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "HYDRATION_METRICS")
            .expect("the metrics diagnostic");
        let bytes = metrics.source_bytes.unwrap();
        let records = metrics.records_parsed.unwrap();
        assert!(
            bytes >= transcript_bytes + stream_bytes,
            "{bytes} must cover the transcript ({transcript_bytes}) and the stream ({stream_bytes})"
        );
        // …and the sidecars on top of those two.
        let mut expected = transcript_bytes + stream_bytes;
        for name in ["summary.json", "signals.json", "prompt_context.json"] {
            expected += fs::metadata(session_dir.join(name)).unwrap().len() as i64;
        }
        expected += fs::metadata(session_dir.join("compaction_checkpoints/1789560090000.json"))
            .unwrap()
            .len() as i64;
        expected += fs::metadata(session_dir.join("subagents/agent-review.json"))
            .unwrap()
            .len() as i64;
        assert_eq!(bytes, expected);
        // 17 transcript records + 16*64 stream records + one per JSON
        // sidecar: summary, signals, prompt context, one checkpoint, one
        // subagent entry.
        assert_eq!(records, 17 + 16 * 64 + 5);
        // The checkpoint stores the same numbers the diagnostic reported.
        let conn = open_db(&db).unwrap();
        let stored: (i64, i64) = conn
            .query_row(
                "SELECT source_bytes, records_parsed FROM session_hydration_checkpoints \
                 WHERE source = 'grok' AND session_id = 'grok-evt-0001'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, (bytes, records));
    }

    /// Stamping a Grok session must cost metadata, not its update stream.
    ///
    /// Discovery stamps every candidate on every run and plain `sync` stamps
    /// every session directory it walks, so a stamp that reads file contents
    /// turns a scan of a store into a parse of it — and the whole point of an
    /// `unchanged` short-circuit is to avoid exactly that read.
    #[test]
    fn stamping_a_grok_session_reads_no_file_contents() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let updates = chat.parent().unwrap().join("updates.jsonl");
        fs::write(&updates, fs::read_to_string(&updates).unwrap().repeat(64)).unwrap();
        let stream_bytes = fs::metadata(&updates).unwrap().len();

        // The stamp itself, as discovery and sync take it.
        reset_content_bytes_read();
        let (stamp, modified) = crate::grok_session_stamp_and_modified(&chat).unwrap();
        assert_eq!(
            content_bytes_read(),
            0,
            "stamping must not read a byte of any file's contents"
        );
        assert!(!stamp.is_empty() && modified.is_some());

        // An unchanged hydration: same guarantee, because the short-circuit
        // runs before anything parses.
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);
        let parsed =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(parsed.status, "hydrated");

        reset_content_bytes_read();
        let cached =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(cached.status, "unchanged");
        assert_eq!(
            content_bytes_read(),
            0,
            "an unchanged hydration must not read the files it decided not to parse"
        );
        // …and it still reports the size of what is on disk, and the record
        // count the parse that produced the stored evidence measured.
        let metrics = |result: &HydrateSessionResult| {
            let diagnostic = result
                .diagnostics
                .iter()
                .find(|diagnostic| diagnostic.code == "HYDRATION_METRICS")
                .expect("the metrics diagnostic");
            (
                diagnostic.source_bytes.unwrap(),
                diagnostic.records_parsed.unwrap(),
            )
        };
        assert_eq!(metrics(&cached), metrics(&parsed));

        // The positive control: the path that does parse still reads it all,
        // so the assertions above measure laziness and not a broken meter.
        reset_content_bytes_read();
        let counted = crate::grok_source_records(&chat).unwrap();
        assert!(
            content_bytes_read() >= stream_bytes,
            "the content pass must read the stream it counts"
        );
        assert_eq!(counted, metrics(&parsed).1);
    }

    /// After a rewrite the directory *is* the session. A merge that keeps the
    /// old end time and the old model reports a session that no longer exists
    /// in the files it points at.
    #[test]
    fn a_rewritten_grok_session_replaces_its_catalog_row() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let session_dir = chat.parent().unwrap().to_path_buf();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        let row =
            |conn: &Connection| -> (Option<i64>, Option<i64>, Option<String>, Option<String>) {
                conn.query_row(
                    "SELECT first_activity_ms, last_activity_ms, last_assistant_text, models_json \
                 FROM sessions WHERE source = 'grok' AND session_id = 'grok-evt-0001'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap()
            };
        let first_prompt = |conn: &Connection| -> Option<String> {
            conn.query_row(
                "SELECT first_prompt FROM sessions WHERE source = 'grok' AND session_id = 'grok-evt-0001'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        // The positive control: the first read really did record the later end
        // time and the model, so the assertions below are a change and not a
        // coincidence.
        assert_eq!(
            row(&conn),
            (
                Some(1_789_560_000_000),
                Some(1_789_560_138_000),
                Some("The test fails; fixing next.".to_string()),
                Some(r#"["grok-4-build"]"#.to_string()),
            )
        );
        assert_eq!(
            first_prompt(&conn).as_deref(),
            Some("add a retry to the http client")
        );

        // Compaction: the session now starts later, ends earlier, and its
        // remaining records name no model and carry no assistant prose.
        fs::write(
            &chat,
            concat!(
                r#"{"type":"user","content":"<user_query>now a test</user_query>"}"#,
                "\n",
                r#"{"type":"assistant","content":"","tool_calls":[{"id":"call_write_1","name":"Write","arguments":{"path":"/tmp/demo/test/http.test.ts"}}]}"#,
                "\n"
            ),
        )
        .unwrap();
        fs::write(
            session_dir.join("updates.jsonl"),
            concat!(
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"eventId":"ev_0009","agentTimestampMs":1789560120000,"turnStartMs":1789560120000}}}"#,
                "\n",
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"call_write_1"},"_meta":{"agentTimestampMs":1789560129000,"turnStartMs":1789560120000}}}"#,
                "\n"
            ),
        )
        .unwrap();

        // The summary is rewritten too, and no longer names a model: with the
        // model still in `info.model` the session really did run it, and
        // clearing the column would be the wrong answer.
        fs::write(
            session_dir.join("summary.json"),
            br#"{"info":{"id":"grok-evt-0001","cwd":"/tmp/demo"},"head_branch":"feat/http-retry","created_at":"2026-09-16T12:00:00.000Z"}"#,
        )
        .unwrap();
        hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path()).unwrap();
        assert_eq!(
            row(&conn),
            (
                Some(1_789_560_120_000),
                Some(1_789_560_129_000),
                None,
                None,
            ),
            "the catalog row must be what the directory now says, not a merge with what it used to say"
        );
        assert_eq!(
            first_prompt(&conn).as_deref(),
            Some("now a test"),
            "compaction that drops the original first turn must not keep it in the catalog"
        );
    }

    /// A relationship recorded by somebody else about this session is not this
    /// session's to delete.
    #[test]
    fn grok_re_ingestion_keeps_a_relationship_another_session_owns() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        crate::record_relationship(
            &conn,
            &crate::ObservedRelationship {
                source: "grok",
                parent_session_id: "grok-parent-0000",
                child_session_id: Some("grok-evt-0001"),
                relationship: "delegated",
                child_agent_type: None,
                child_agent_name: None,
                child_model: None,
                spawn_depth: Some(1),
                evidence_kind: "grok_subagent_dir",
                evidence_locator: Some("/elsewhere/subagents/child.json"),
                evidence_ref: None,
                child_has_events: false,
                spawned_at_ms: None,
                origin_session_id: None,
                relationship_uid: None,
            },
        )
        .unwrap();
        drop(conn);

        hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        let parents: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships \
                 WHERE source = 'grok' AND parent_session_id = 'grok-parent-0000'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(parents, 1, "this session does not own its parent's record");
        assert_eq!(grok_relationship_count(&conn), 1);
    }

    /// A new `compaction_checkpoints/` entry, an edited `signals.json` or a
    /// new `subagents/` entry is new evidence even when the transcript and the
    /// update stream are byte-identical.
    #[test]
    fn grok_hydration_re_reads_when_only_an_extra_file_changed() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let session_dir = chat.parent().unwrap().to_path_buf();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        assert_eq!(grok_marker_count(&conn, "compaction_boundary"), 1);
        // Positive control: with nothing touched, the read really is skipped.
        let untouched =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(untouched.status, "unchanged");

        fs::write(
            session_dir.join("compaction_checkpoints/1789560200000.json"),
            br#"{"created_at":"2026-09-16T12:03:20.000Z","reason":"manual"}"#,
        )
        .unwrap();
        let after_checkpoint =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(after_checkpoint.status, "updated");
        assert_eq!(grok_marker_count(&conn, "compaction_boundary"), 2);

        fs::write(
            session_dir.join("signals.json"),
            br#"{"contextTokensUsed":9210,"turnCount":3,"compactionCount":2,"toolFailures":1}"#,
        )
        .unwrap();
        let after_signals =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(after_signals.status, "updated");
        let signals = crate::session_markers(&conn, "grok", "grok-evt-0001")
            .unwrap()
            .into_iter()
            .find(|marker| marker.kind == "signals")
            .unwrap();
        assert_eq!(
            signals.text.as_deref(),
            Some("turns=3 compactions=2 context_tokens_used=9210")
        );

        fs::write(
            session_dir.join("subagents/agent-second.json"),
            br#"{"session_id":"grok-evt-0001-second","agent_type":"tester"}"#,
        )
        .unwrap();
        let after_subagent =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(after_subagent.status, "updated");
        assert_eq!(grok_relationship_count(&conn), 2);
    }

    /// The caveats belong to the stored evidence, not to the run that parsed
    /// it: a reader of an `unchanged` result sees the same rows and has to be
    /// told the same things about them.
    #[test]
    fn unchanged_grok_hydration_still_reports_what_grok_does_not_record() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);

        let parsed =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        let cached =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(parsed.status, "hydrated");
        assert_eq!(cached.status, "unchanged");

        let codes = |result: &HydrateSessionResult| {
            result
                .diagnostics
                .iter()
                .filter(|diagnostic| diagnostic.code.starts_with("GROK_"))
                .map(|diagnostic| (diagnostic.code.clone(), diagnostic.message.clone()))
                .collect::<Vec<_>>()
        };
        assert!(
            codes(&parsed)
                .iter()
                .any(|(code, _)| code == "GROK_USAGE_CONTEXT_PROXY_ONLY"),
            "{:?}",
            parsed.diagnostics
        );
        assert_eq!(
            codes(&cached),
            codes(&parsed),
            "a cached read must report exactly what the parse reported"
        );
        assert!(codes(&cached)
            .iter()
            .any(|(_, message)| message.contains("latest: 9210")));
    }

    /// Grok hydration covers every evidence kind, and still names the usage
    /// proxy.
    ///
    /// Capability is defined by `coverage` (contract 3 / #169): Grok's parser
    /// writes history, events, tool calls, file edits and relationships, so
    /// the result is `full`. Usage is not one of those kinds. Grok writes no
    /// per-turn billing tokens -- `updates.jsonl` carries a running context
    /// total, recorded as a labelled proxy -- and that absence is the
    /// `GROK_USAGE_CONTEXT_PROXY_ONLY` diagnostic, on cached reads as well as
    /// parsed ones. Reporting `partial` while covering every kind would be a
    /// contract mismatch the TypeScript SDK rejects.
    ///
    /// The positive control is a Claude session: it is also `full`, and the
    /// Grok caveat must not leak onto it.
    #[test]
    fn grok_hydration_names_the_usage_proxy_on_a_full_coverage_result() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);

        let parsed =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(parsed.status, "hydrated");
        assert_eq!(parsed.capability, "full");
        assert_eq!(parsed.coverage, FULL_SESSION_KINDS.to_vec());
        let reason = parsed
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "GROK_USAGE_CONTEXT_PROXY_ONLY")
            .unwrap_or_else(|| panic!("no reason given: {:?}", parsed.diagnostics));
        assert!(
            reason.message.contains("context"),
            "the reason has to name the proxy: {}",
            reason.message
        );

        // The caveat is a fact about the stored rows, so the cached read
        // reports the same capability and the same reason.
        let cached =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(cached.status, "unchanged");
        assert_eq!(cached.capability, "full");
        assert!(cached
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "GROK_USAGE_CONTEXT_PROXY_ONLY"));

        // The positive control: a provider that does record per-turn tokens
        // still reports `full` through the same code path.
        let transcript = dir.path().join(".claude/projects/app/session-1.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            concat!(
                "{\"sessionId\":\"session-1\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"first prompt\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
                "{\"sessionId\":\"session-1\",\"uuid\":\"a1\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"done\"}],\"usage\":{\"input_tokens\":11,\"output_tokens\":7}},\"timestamp\":\"2026-08-31T10:00:01Z\"}\n",
            ),
        )
        .unwrap();
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);
        let claude =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert_eq!(claude.capability, "full");
        assert!(
            !claude
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code.starts_with("GROK_")),
            "the Grok caveat must not leak onto another source: {:?}",
            claude.diagnostics
        );
    }

    /// A checkpoint written before the diagnostics were persisted must not
    /// leave a Grok session looking caveat-free.
    #[test]
    fn a_pre_existing_checkpoint_still_reports_the_grok_usage_caveat() {
        let dir = tempfile::tempdir().unwrap();
        let chat = grok_fixture_home(dir.path(), "events-session");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "grok", "grok-evt-0001", Some(&chat));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        // What an older release left behind: a checkpoint with no diagnostics.
        conn.execute(
            "UPDATE session_hydration_checkpoints SET source_diagnostics_json = NULL \
             WHERE source = 'grok'",
            [],
        )
        .unwrap();
        drop(conn);

        let cached =
            hydrate_session_at_with_home(&db, &options("grok", "grok-evt-0001"), dir.path())
                .unwrap();
        assert_eq!(cached.status, "unchanged");
        let usage = cached
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "GROK_USAGE_CONTEXT_PROXY_ONLY")
            .expect("the usage caveat, rebuilt from the stored rows");
        // Rebuilt from the stored `token_json`, not from a parse.
        assert!(usage.message.contains("latest: 9210"), "{}", usage.message);
    }

    fn grok_has_tool_call(conn: &Connection, tool_use_id: &str) -> bool {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM tool_calls WHERE source = 'grok' AND tool_use_id = ?)",
            params![tool_use_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn grok_has_event(conn: &Connection, event_uid: &str) -> bool {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE source = 'grok' AND event_uid = ?)",
            params![event_uid],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn grok_has_file_edit(conn: &Connection, file_path: &str) -> bool {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM file_edits WHERE source = 'grok' AND file_path = ?)",
            params![file_path],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn grok_marker_count(conn: &Connection, kind: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM session_markers WHERE source = 'grok' AND kind = ?",
            params![kind],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn grok_relationship_count(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM session_relationships \
             WHERE source = 'grok' AND parent_session_id = 'grok-evt-0001'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn options(source: &str, session_id: &str) -> HydrateSessionOptions {
        HydrateSessionOptions {
            source: source.to_string(),
            session_id: session_id.to_string(),
            scope: SessionScope::Local,
            include_related: true,
        }
    }

    #[test]
    fn catalog_prerequisite_is_stable_and_does_not_discover() {
        let dir = tempfile::tempdir().unwrap();
        let error = hydrate_session_at_with_home(
            &dir.path().join("history.db"),
            &options("claude", "missing"),
            dir.path(),
        )
        .unwrap_err();
        assert!(format!("{error:#}").starts_with("SESSION_NOT_FOUND:"));
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    /// Copy a checked-in Cursor fixture to where a real install would put it.
    fn cursor_fixture_home(home: &Path, fixture: &str, session_id: &str) -> PathBuf {
        let transcript = home
            .join(".cursor/projects/home-dev-demo/agent-transcripts")
            .join(session_id)
            .join(format!("{session_id}.jsonl"));
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/cursor")
                .join(fixture),
            &transcript,
        )
        .unwrap();
        transcript
    }

    #[test]
    fn cursor_hydration_indexes_events_tools_and_edits_with_recorded_turn_times() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = cursor_fixture_home(dir.path(), "observed-3.13.25.jsonl", "cur-1");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "cursor", "cur-1", Some(&transcript));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("cursor", "cur-1"), dir.path()).unwrap();
        assert_eq!(result.status, "hydrated");
        assert!(result.evidence.events > 0, "{:?}", result.evidence);
        assert!(result.evidence.tool_calls > 0, "{:?}", result.evidence);
        assert!(result.evidence.file_edits > 0, "{:?}", result.evidence);
        // Every turn here carries a readable `<timestamp>`, so nothing fell
        // back to the file mtime and the diagnostic must stay silent.
        assert!(
            !result
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "CURSOR_TIMESTAMP_FROM_MTIME"),
            "{:?}",
            result.diagnostics
        );

        let conn = open_db(&db).unwrap();
        let session: (Option<i64>, Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT first_activity_ms, last_activity_ms, last_assistant_text \
                 FROM sessions WHERE source = 'cursor' AND session_id = 'cur-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(session.0, Some(1_789_587_420_000));
        assert_eq!(session.1, Some(1_789_587_660_000));
        assert_eq!(session.2.as_deref(), Some("Fixed the failing assertion."));

        // Re-hydrating the same unchanged file neither duplicates evidence nor
        // duplicates the prompts it rebuilds.
        let again =
            hydrate_session_at_with_home(&db, &options("cursor", "cur-1"), dir.path()).unwrap();
        assert_eq!(again.status, "unchanged");
        assert_eq!(again.evidence.events, result.evidence.events);
        let prompts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source = 'cursor'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prompts, 2);
    }

    #[test]
    fn cursor_hydration_reports_the_mtime_fallback_and_unlinked_subagent_spawns() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = cursor_fixture_home(dir.path(), "legacy-string-content.jsonl", "cur-legacy");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "cursor", "cur-legacy", Some(&legacy));
        drop(conn);
        let result =
            hydrate_session_at_with_home(&db, &options("cursor", "cur-legacy"), dir.path())
                .unwrap();
        assert!(
            result
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "CURSOR_TIMESTAMP_FROM_MTIME"),
            "a transcript with no readable turn time must say that it used the mtime: {:?}",
            result.diagnostics
        );

        let extended = cursor_fixture_home(dir.path(), "extended-unverified.jsonl", "cur-ext");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "cursor", "cur-ext", Some(&extended));
        drop(conn);
        let result =
            hydrate_session_at_with_home(&db, &options("cursor", "cur-ext"), dir.path()).unwrap();
        assert!(
            result
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "CURSOR_SUBAGENT_SPAWN_UNLINKED"),
            "a recorded spawn with no child id must be reported, not silently dropped: {:?}",
            result.diagnostics
        );
        // Cursor never names the child, so no relationship row is invented.
        let conn = open_db(&db).unwrap();
        let relationships: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships WHERE source = 'cursor'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(relationships, 0);
        assert_eq!(
            crate::relationships::relationship_capabilities("cursor").stable_child_identity,
            "never"
        );

        // Diagnostics describe the stored evidence, not this run. Re-hydrating
        // the same file must still report them.
        let again =
            hydrate_session_at_with_home(&db, &options("cursor", "cur-ext"), dir.path()).unwrap();
        assert_eq!(again.status, "unchanged");
        assert!(
            again
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "CURSOR_SUBAGENT_SPAWN_UNLINKED"),
            "unchanged hydration dropped the unlinked-spawn diagnostic: {:?}",
            again.diagnostics
        );
        let again_mtime =
            hydrate_session_at_with_home(&db, &options("cursor", "cur-legacy"), dir.path())
                .unwrap();
        assert_eq!(again_mtime.status, "unchanged");
        assert!(
            again_mtime
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "CURSOR_TIMESTAMP_FROM_MTIME"),
            "unchanged hydration dropped the mtime-fallback diagnostic: {:?}",
            again_mtime.diagnostics
        );
    }

    /// Targeted hydration re-reads from offset 0 and used not to move the
    /// Cursor byte checkpoint. A later incremental sync then inserted every
    /// untimed prompt past the old offset again, under the new mtime.
    ///
    /// Positive control: without `record_cursor_hydrate_checkpoint` this
    /// failed with `hydrate then sync duplicated untimed prompts: left: 2,
    /// right: 1`.
    #[test]
    fn hydrating_a_cursor_session_does_not_let_later_sync_duplicate_untimed_prompts() {
        let dir = tempfile::tempdir().unwrap();
        let body = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<user_query>do it</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"text","text":"done"}]}}"#,
            "\n"
        );
        let transcript = cursor_fixture_home(dir.path(), "legacy-string-content.jsonl", "s-dup");
        fs::write(&transcript, body).unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "cursor", "s-dup", Some(&transcript));
        let mut state = Map::new();
        super::test_sync_cursor(&conn, dir.path(), &mut state);
        let prompts_after_sync: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source = 'cursor' AND session_id = 's-dup'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prompts_after_sync, 1);
        drop(conn);

        hydrate_session_at_with_home(&db, &options("cursor", "s-dup"), dir.path()).unwrap();

        // Move the mtime without changing bytes, so an untimed re-insert would
        // be a new history row rather than folding into the hydrated one.
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(60);
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&transcript)
            .unwrap();
        file.set_times(fs::FileTimes::new().set_modified(later))
            .unwrap();
        drop(file);

        let conn = open_db(&db).unwrap();
        let mut state = super::test_load_sync_state(&db);
        super::test_sync_cursor(&conn, dir.path(), &mut state);
        let prompts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source = 'cursor' AND session_id = 's-dup'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prompts, 1, "hydrate then sync duplicated untimed prompts");
    }

    #[test]
    fn claude_hydration_is_incremental_idempotent_and_ignores_partial_tail() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join(".claude/projects/app/session-1.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            concat!(
                "{\"sessionId\":\"session-1\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"first prompt\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
                "{\"sessionId\":\"session-1\",\"uuid\":\"a1\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"done\"}]},\"timestamp\":\"2026-08-31T10:00:01Z\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            transcript.parent().unwrap().join("agent-child.jsonl"),
            concat!(
                "{\"sessionId\":\"session-1\",\"uuid\":\"side-u\",\"isSidechain\":true,\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"delegated instruction\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
                "{\"sessionId\":\"session-1\",\"uuid\":\"side-a\",\"isSidechain\":true,\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"side result\"},\"timestamp\":\"2026-08-31T10:00:01Z\"}\n",
            ),
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert_eq!(first.status, "hydrated");
        assert_eq!(first.evidence.prompts, 1);
        assert_eq!(first.evidence.events, 3);
        let presence_stamp: String = open_db(&db)
            .unwrap()
            .query_row(
                "SELECT source_stamp FROM session_presences WHERE source='claude' AND session_id='session-1' AND location='local'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        // Carried from the catalog row seeded above rather than recomputed, so this
        // stays on that fixture's literal across scanner-version bumps.
        assert_eq!(presence_stamp, "v2:test-discovery-stamp");

        let second =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert_eq!(second.status, "unchanged");
        assert_eq!(second.evidence.events, 3);

        open_db(&db)
            .unwrap()
            .execute(
                "UPDATE observation_hydration_checkpoints SET parser_version = 0 WHERE source='claude' AND session_id='session-1'",
                [],
            )
            .unwrap();
        let reparsed =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert_eq!(reparsed.status, "updated");

        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        write!(file, "{{\"sessionId\":\"session-1\",\"uuid\":\"u2\"").unwrap();
        drop(file);
        let partial =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert_eq!(partial.status, "updated");
        assert_eq!(partial.evidence.events, 3);

        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        writeln!(file, ",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"second prompt\"}},\"timestamp\":\"2026-08-31T10:00:02Z\"}}").unwrap();
        drop(file);
        let appended =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert_eq!(appended.status, "updated");
        assert_eq!(appended.evidence.prompts, 2);
        assert_eq!(appended.evidence.events, 4);
    }

    /// Write a cursor transcript at the layout its adapter enumerates and
    /// return the path the catalog stores as the locator.
    fn cursor_transcript(home: &Path, session_id: &str, prompt: &str) -> PathBuf {
        let path = home
            .join(".cursor/projects/work-app/agent-transcripts")
            .join(session_id)
            .join(format!("{session_id}.jsonl"));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            format!(
                "{}\n",
                serde_json::json!({
                    "role": "user",
                    "message": {"content": prompt},
                })
            ),
        )
        .unwrap();
        path
    }

    /// Write an OpenCode store with one user prompt. OpenCode is the remaining
    /// prompt-only local parser: Cursor and Grok both write events now.
    fn opencode_prompt_only(home: &Path, session_id: &str, prompt: &str) -> PathBuf {
        let source = home.join(".local/share/opencode/opencode.db");
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        let src = Connection::open(&source).unwrap();
        let escaped = prompt.replace('\'', "''");
        src.execute_batch(&format!(
            "CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER); \
             CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT); \
             CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT); \
             INSERT INTO session VALUES ('{session_id}', '/work/app', 1); \
             INSERT INTO message VALUES ('m1', '{session_id}', 1, '{{\"role\":\"user\"}}'); \
             INSERT INTO part VALUES ('p1', 'm1', '{session_id}', 2, '{{\"type\":\"text\",\"text\":\"{escaped}\"}}');"
        ))
        .unwrap();
        drop(src);
        source
    }

    /// A completed prompt-only hydration is `partial`, and says which evidence
    /// nobody looked for. Reporting `full` here is the defect contract 3 fixes:
    /// the SDK ranks merges on `capability`, so "prompts only" outranked a
    /// remote presence that had the events.
    #[test]
    fn prompt_only_providers_report_partial_capability_and_name_what_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = opencode_prompt_only(dir.path(), "oc-1", "opencode prompt");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "opencode", "oc-1", Some(&store));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("opencode", "oc-1"), dir.path()).unwrap();
        assert_eq!(result.contract_version, 3);
        assert_eq!(result.status, "hydrated");
        assert_eq!(result.capability, "partial");
        assert_eq!(result.coverage, vec![EvidenceKind::History]);
        // The prompt really was indexed: `partial` is about the kinds nobody
        // parses, not about this pass having failed.
        assert_eq!(result.evidence.prompts, 1);
        let partial = result
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "HYDRATION_PARTIAL_COVERAGE")
            .expect("a partial hydration names the evidence it does not cover");
        assert!(
            partial
                .message
                .contains("session_event, tool_call, file_edit, relationship"),
            "{}",
            partial.message
        );
        assert!(
            partial.message.contains("covers history"),
            "{}",
            partial.message
        );

        // Unchanged re-hydration reports the same capability: a consumer that
        // polls must not see the claim change under it.
        let unchanged =
            hydrate_session_at_with_home(&db, &options("opencode", "oc-1"), dir.path()).unwrap();
        assert_eq!(unchanged.status, "unchanged");
        assert_eq!(unchanged.capability, "partial");
        assert_eq!(unchanged.coverage, vec![EvidenceKind::History]);
        assert!(unchanged
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "HYDRATION_PARTIAL_COVERAGE"));
    }

    /// A provider whose parser produces every kind still reports `full`, with
    /// the coverage that justifies it and no partial-coverage diagnostic.
    #[test]
    fn full_coverage_providers_still_report_full_capability() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join(".claude/projects/app/full-1.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            "{\"sessionId\":\"full-1\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"prompt\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "full-1", Some(&transcript));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("claude", "full-1"), dir.path()).unwrap();
        assert_eq!(result.capability, "full");
        assert_eq!(result.discovery_state, "full");
        assert_eq!(result.coverage, FULL_SESSION_KINDS.to_vec());
        assert!(!result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "HYDRATION_PARTIAL_COVERAGE"));
    }

    /// `discovery_state` answers "indexed through the recorded stamp", not
    /// "every evidence kind exists". A partial hydration still reaches `full`
    /// there -- and the reported value is read back off the row rather than
    /// asserted, so the two cannot disagree.
    ///
    /// The risk that buys is a stale short-circuit: `discovery_state = 'full'`
    /// is one of the conditions for skipping re-hydration. The parser version
    /// has to be the thing that breaks the tie after a parser upgrade, or a
    /// prompt-only provider that grows a real parser would never re-parse the
    /// sessions it already touched.
    #[test]
    fn partial_coverage_keeps_discovery_state_full_and_still_reparses_on_parser_bump() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = cursor_transcript(dir.path(), "cursor-2", "cursor prompt");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "cursor", "cursor-2", Some(&transcript));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("cursor", "cursor-2"), dir.path()).unwrap();
        assert_eq!(first.capability, "partial");
        assert_eq!(first.discovery_state, "full");
        let stored: String = open_db(&db)
            .unwrap()
            .query_row(
                "SELECT discovery_state FROM sessions WHERE source='cursor' AND session_id='cursor-2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, first.discovery_state);

        // Same bytes, same parser: the short-circuit is reached.
        assert_eq!(
            hydrate_session_at_with_home(&db, &options("cursor", "cursor-2"), dir.path())
                .unwrap()
                .status,
            "unchanged"
        );

        // A parser upgrade must re-parse even though the row still says full.
        open_db(&db)
            .unwrap()
            .execute(
                "UPDATE observation_hydration_checkpoints SET parser_version = ? \
                 WHERE source='cursor' AND session_id='cursor-2'",
                params![HYDRATION_PARSER_VERSION - 1],
            )
            .unwrap();
        let reparsed =
            hydrate_session_at_with_home(&db, &options("cursor", "cursor-2"), dir.path()).unwrap();
        assert_eq!(reparsed.status, "updated");
        assert_eq!(reparsed.discovery_state, "full");
        let parser_version: i64 = open_db(&db)
            .unwrap()
            .query_row(
                "SELECT parser_version FROM observation_hydration_checkpoints \
                 WHERE source='cursor' AND session_id='cursor-2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(parser_version, HYDRATION_PARSER_VERSION);
    }

    /// `include_related: false` is honoured literally by the acquisition --
    /// Claude sidecars are not walked, Codex child rollouts are not read -- so
    /// the result must not claim relationship coverage. Reporting `full` there
    /// tells a merger that unexamined delegation was fully indexed.
    #[test]
    fn declining_related_evidence_drops_relationship_coverage_for_both_full_providers() {
        let dir = tempfile::tempdir().unwrap();
        let claude = dir.path().join(".claude/projects/app/root-1.jsonl");
        fs::create_dir_all(claude.parent().unwrap()).unwrap();
        fs::write(
            &claude,
            "{\"sessionId\":\"root-1\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"continuedFromSessionId\":\"prior-claude\",\"message\":{\"role\":\"user\",\"content\":\"prompt\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
        )
        .unwrap();
        let codex = dir
            .path()
            .join(".codex/sessions/2026/08/31/rollout-root-2.jsonl");
        fs::create_dir_all(codex.parent().unwrap()).unwrap();
        fs::write(
            &codex,
            concat!(
                "{\"timestamp\":\"2026-08-31T11:00:00.000Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"root-2\",\"cwd\":\"/work/app\",\"continuedFromSessionId\":\"prior-codex\"}}\n",
                "{\"timestamp\":\"2026-08-31T11:00:01.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"prompt\"}}\n",
            ),
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "root-1", Some(&claude));
        catalog_row(&conn, "codex", "root-2", Some(&codex));
        drop(conn);

        for (source, session_id) in [("claude", "root-1"), ("codex", "root-2")] {
            let mut request = options(source, session_id);
            request.include_related = false;
            let alone = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
            assert_eq!(alone.capability, "partial", "{source} thread-only");
            assert_eq!(
                alone.coverage,
                vec![
                    EvidenceKind::History,
                    EvidenceKind::SessionEvent,
                    EvidenceKind::ToolCall,
                    EvidenceKind::FileEdit,
                ],
                "{source} thread-only coverage excludes relationship"
            );
            let partial = alone
                .diagnostics
                .iter()
                .find(|diagnostic| diagnostic.code == "HYDRATION_PARTIAL_COVERAGE")
                .unwrap_or_else(|| panic!("{source} thread-only names its missing kinds"));
            assert!(
                partial.message.contains("no relationship"),
                "{}",
                partial.message
            );
            // The reason matters: nothing here says this provider cannot record
            // delegation, only that this request did not ask for it.
            assert!(
                partial.message.contains("include_related is off"),
                "{}",
                partial.message
            );
            assert!(!alone
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code.starts_with("RELATIONSHIP_CONTINUITY_")));
            let conn = open_db(&db).unwrap();
            let banked: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM session_continuity_evidence WHERE source = ?",
                    [source],
                    |row| row.get(0),
                )
                .unwrap();
            let edges: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM session_relationships \
                     WHERE source = ? AND relationship = 'continuation'",
                    [source],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!((banked, edges), (0, 0), "{source} thread-only");
            drop(conn);

            // Asking for the related evidence restores the claim, and the same
            // provider is `full` again.
            request.include_related = true;
            let related = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
            assert_eq!(related.capability, "full", "{source} with related");
            assert_eq!(related.coverage, FULL_SESSION_KINDS.to_vec());
            assert!(!related
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "HYDRATION_PARTIAL_COVERAGE"));
            let conn = open_db(&db).unwrap();
            let parent: String = conn
                .query_row(
                    "SELECT parent_session_id FROM session_relationships \
                     WHERE source = ? AND child_session_id = ? AND relationship = 'continuation'",
                    params![source, session_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(parent, format!("prior-{source}"));
        }
    }

    #[test]
    fn version_four_checkpoints_reparse_to_bank_continuity() {
        let dir = tempfile::tempdir().unwrap();
        let claude = claude_fixture(dir.path(), "explicit-line-relationships.jsonl");
        let codex = dir
            .path()
            .join(".codex/sessions/2026/08/31/rollout-forked.jsonl");
        fs::create_dir_all(codex.parent().unwrap()).unwrap();
        fs::write(
            &codex,
            concat!(
                "{\"timestamp\":\"2026-08-31T11:00:00Z\",\"type\":\"session_meta\",",
                "\"payload\":{\"id\":\"forked\",\"cwd\":\"/work/app\",",
                "\"continuedFromSessionId\":\"prior\"}}\n",
                "{\"timestamp\":\"2026-08-31T11:00:01Z\",\"type\":\"event_msg\",",
                "\"payload\":{\"type\":\"user_message\",\"message\":\"go\"}}\n",
            ),
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(
            &conn,
            "claude",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            Some(&claude),
        );
        catalog_row(&conn, "codex", "forked", Some(&codex));
        drop(conn);

        for (source, session_id) in [
            ("claude", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
            ("codex", "forked"),
        ] {
            let request = options(source, session_id);
            hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
            let conn = open_db(&db).unwrap();
            conn.execute(
                "UPDATE observation_hydration_checkpoints SET parser_version = 4 \
                 WHERE source = ? AND session_id = ? AND location = 'local'",
                params![source, session_id],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM session_relationships WHERE source = ? \
                 AND relationship IN ('continuation', 'fork', 'resume')",
                [source],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM session_continuity_evidence WHERE source = ?",
                [source],
            )
            .unwrap();
            drop(conn);

            let repaired = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
            assert_eq!(repaired.status, "updated", "{source} v4 checkpoint");
            let conn = open_db(&db).unwrap();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM session_relationships WHERE source = ? \
                     AND child_session_id = ? AND relationship = 'continuation'",
                    params![source, session_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "{source} continuity restored");
        }
    }

    /// Dropping relationship coverage is scoped to providers that would
    /// otherwise have claimed it; a prompt-only provider reports the same
    /// thing either way.
    #[test]
    fn declining_related_evidence_does_not_change_a_prompt_only_provider() {
        let dir = tempfile::tempdir().unwrap();
        let store = opencode_prompt_only(dir.path(), "oc-3", "opencode prompt");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "opencode", "oc-3", Some(&store));
        drop(conn);

        let mut request = options("opencode", "oc-3");
        request.include_related = false;
        let alone = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert_eq!(alone.capability, "partial");
        assert_eq!(alone.coverage, vec![EvidenceKind::History]);
    }

    /// The rule the SDK re-derives from `coverage` to validate a result. All
    /// three branches are asserted here because the empty one is unreachable
    /// through the public path, so nothing else would catch it changing.
    #[test]
    fn capability_is_one_rule_over_the_covered_kinds() {
        assert_eq!(capability_for(FULL_SESSION_KINDS), "full");
        assert_eq!(capability_for(&[EvidenceKind::History]), "partial");
        assert_eq!(
            capability_for(&[
                EvidenceKind::History,
                EvidenceKind::SessionEvent,
                EvidenceKind::ToolCall,
                EvidenceKind::FileEdit,
            ]),
            "partial"
        );
        // Covering nothing is shallow, not partial: `partial` would imply some
        // evidence kind was indexed.
        assert_eq!(capability_for(&[]), "shallow_only");
        // An extra kind beyond the full set does not stop it being full.
        assert_eq!(
            capability_for(&[
                EvidenceKind::History,
                EvidenceKind::SessionEvent,
                EvidenceKind::ToolCall,
                EvidenceKind::FileEdit,
                EvidenceKind::Relationship,
                EvidenceKind::CommitLink,
            ]),
            "full"
        );
    }

    fn relationship_rows(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM session_relationships", [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    /// Declining related evidence has to reach *every* path that writes a
    /// relationship, not just the ones named after delegation. A Claude
    /// transcript carrying `remoteSessionId` records a `materialized_local`
    /// edge into `session_relationships` -- the same table the `Relationship`
    /// kind is defined over -- so writing it while reporting no relationship
    /// coverage leaves the result and the database disagreeing.
    #[test]
    fn declining_related_evidence_skips_the_claude_materialization_edge() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join(".claude/projects/app/local-1.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            "{\"sessionId\":\"local-1\",\"remoteSessionId\":\"session_01remote\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"prompt\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "local-1", Some(&transcript));
        // The remote counterpart is already known, which is what makes the
        // materialization edge eligible to be written at all.
        conn.execute(
            "INSERT INTO session_presences (source, session_id, location, discovery_state) \
             VALUES ('claude', 'session_01remote', 'remote', 'shallow')",
            [],
        )
        .unwrap();
        drop(conn);

        let mut request = options("claude", "local-1");
        request.include_related = false;
        let declined = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert!(!declined.coverage.contains(&EvidenceKind::Relationship));
        assert_eq!(
            relationship_rows(&open_db(&db).unwrap()),
            0,
            "a declined acquisition wrote a relationship it reported not covering"
        );

        // Control: asking for related evidence still records the edge, so the
        // assertion above is about the option and not about a transcript whose
        // remote counterpart was never linkable.
        request.include_related = true;
        let requested = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert!(requested.coverage.contains(&EvidenceKind::Relationship));
        assert_eq!(relationship_rows(&open_db(&db).unwrap()), 1);
    }

    fn codex_rollout(path: &Path, id: &str, parent: Option<&str>, ts: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let meta = match parent {
            Some(parent) => format!(
                "{{\"timestamp\":\"{ts}\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\",\"parent_thread_id\":\"{parent}\",\"cwd\":\"/work/app\",\"source\":{{\"subagent\":{{\"other\":\"guardian\"}}}}}}}}\n"
            ),
            None => format!(
                "{{\"timestamp\":\"{ts}\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\",\"cwd\":\"/work/app\"}}}}\n"
            ),
        };
        fs::write(
            path,
            format!(
                "{meta}{{\"timestamp\":\"{ts}\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"user_message\",\"message\":\"{id} prompt\"}}}}\n"
            ),
        )
        .unwrap();
    }

    /// Codex rollouts are partitioned by the date they started, so a child
    /// spawned after midnight lands in the next day's directory. Scanning only
    /// the parent's own directory never finds it, while Codex declares full
    /// relationship coverage and the result reports `full` -- a complete claim
    /// over a child nobody looked for.
    #[test]
    fn codex_children_are_found_across_the_date_directory_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let sessions = dir.path().join(".codex/sessions");
        let root = sessions.join("2026/09/20/rollout-root.jsonl");
        codex_rollout(&root, "root", None, "2026-09-20T23:59:00Z");
        // Same day: the case that already worked, kept as the control.
        codex_rollout(
            &sessions.join("2026/09/20/rollout-same-day.jsonl"),
            "same-day",
            Some("root"),
            "2026-09-20T23:59:30Z",
        );
        // Minutes later, but the calendar turned over.
        codex_rollout(
            &sessions.join("2026/09/21/rollout-after-midnight.jsonl"),
            "after-midnight",
            Some("root"),
            "2026-09-21T00:00:30Z",
        );

        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "root", Some(&root));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        assert!(
            result.related_session_ids.contains(&"same-day".to_string()),
            "the same-day child is still found: {:?}",
            result.related_session_ids
        );
        assert!(
            result
                .related_session_ids
                .contains(&"after-midnight".to_string()),
            "a child that started after midnight is in the next date directory: {:?}",
            result.related_session_ids
        );
        // Codex declares relationship coverage, so the acquisition has to be
        // able to reach every eligible child for `full` to be earned.
        assert!(result.coverage.contains(&EvidenceKind::Relationship));
        assert_eq!(result.capability, "full");
    }

    #[test]
    fn old_codex_hydration_bounds_child_search_and_reports_partial_relationship_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let sessions = dir.path().join(".codex/sessions");
        let root = sessions.join("2025/01/01/rollout-root.jsonl");
        codex_rollout(&root, "root", None, "2025-01-01T23:59:00Z");
        codex_rollout(
            &sessions.join("2025/01/02/rollout-next-day.jsonl"),
            "next-day",
            Some("root"),
            "2025-01-02T00:00:30Z",
        );
        let later = sessions.join("2026/09/20/rollout-later.jsonl");
        codex_rollout(&later, "later", Some("root"), "2026-09-20T12:00:00Z");
        // Empty newer year/month directories must not hide the most recent
        // populated date, which is still outside the bounded search.
        fs::create_dir_all(sessions.join("2027/01")).unwrap();
        fs::create_dir_all(sessions.join("2026/10")).unwrap();
        assert!(!codex_child_scan_complete(root.parent().unwrap()).unwrap());
        assert!(!codex_child_candidates(root.parent().unwrap())
            .unwrap()
            .contains(&later));

        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "root", Some(&root));
        drop(conn);
        let result =
            hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        assert!(result.related_session_ids.contains(&"next-day".to_string()));
        assert!(!result.related_session_ids.contains(&"later".to_string()));
        assert!(!result.coverage.contains(&EvidenceKind::Relationship));
        assert_eq!(result.capability, "partial");
    }

    #[test]
    fn codex_children_across_active_and_archived_roots_are_found() {
        let dir = tempfile::tempdir().unwrap();
        let codex = dir.path().join(".codex");
        let root = codex.join("sessions/2026/09/20/rollout-root.jsonl");
        codex_rollout(&root, "root", None, "2026-09-20T23:59:00Z");
        codex_rollout(
            &codex.join("archived_sessions/2026/09/21/rollout-child.jsonl"),
            "archived-child",
            Some("root"),
            "2026-09-21T00:00:30Z",
        );

        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "root", Some(&root));
        drop(conn);
        let found =
            hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        assert!(found
            .related_session_ids
            .contains(&"archived-child".to_string()));
        assert_eq!(found.capability, "full");

        // A later date in either root invalidates the exhaustive coverage
        // claim, even when the selected root has no newer rollouts.
        codex_rollout(
            &codex.join("archived_sessions/2026/09/23/rollout-later.jsonl"),
            "later",
            Some("root"),
            "2026-09-23T10:00:00Z",
        );
        let bounded =
            hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        assert_ne!(bounded.status, "unchanged");
        assert_eq!(bounded.capability, "partial");
        assert!(!bounded.coverage.contains(&EvidenceKind::Relationship));
    }

    #[test]
    fn local_hydration_reports_coverage_option_changes_as_updates() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join(".claude/projects/app/root.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(&transcript, "{\"sessionId\":\"root\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"prompt\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n").unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "root", Some(&transcript));
        drop(conn);

        let mut request = options("claude", "root");
        let full = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert_eq!(full.capability, "full");
        assert_eq!(
            hydrate_session_at_with_home(&db, &request, dir.path())
                .unwrap()
                .status,
            "unchanged"
        );

        request.include_related = false;
        let narrowed = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert_ne!(narrowed.status, "unchanged");
        assert_eq!(narrowed.capability, "partial");
        assert_eq!(
            hydrate_session_at_with_home(&db, &request, dir.path())
                .unwrap()
                .status,
            "unchanged"
        );

        request.include_related = true;
        let widened = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert_ne!(widened.status, "unchanged");
        assert_eq!(widened.capability, "full");
    }

    /// The opt-out explains an absent `relationship` and nothing else. Cursor
    /// is missing that kind because its parser never reads it — a `Task` block
    /// names no child transcript — and blaming the request for that sends the
    /// reader looking for an option to change instead of at the provider.
    #[test]
    fn the_partial_diagnostic_blames_the_opt_out_only_for_what_it_removed() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = cursor_transcript(dir.path(), "cursor-4", "cursor prompt");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "cursor", "cursor-4", Some(&transcript));
        drop(conn);

        let mut request = options("cursor", "cursor-4");
        request.include_related = false;
        let prompt_only = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        let message = prompt_only
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "HYDRATION_PARTIAL_COVERAGE")
            .expect("a partial hydration names its missing kinds")
            .message
            .clone();
        assert!(
            !message.contains("include_related"),
            "cursor has no relationship parser, so the opt-out did not remove it: {message}"
        );

        // Control: a source that does declare delegation, with the same flag
        // off, still attributes the absence to the request.
        let claude = dir.path().join(".claude/projects/app/claude-4.jsonl");
        fs::create_dir_all(claude.parent().unwrap()).unwrap();
        fs::write(
            &claude,
            "{\"sessionId\":\"claude-4\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"prompt\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
        )
        .unwrap();
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "claude-4", Some(&claude));
        drop(conn);
        let mut request = options("claude", "claude-4");
        request.include_related = false;
        let declined = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert!(declined
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "HYDRATION_PARTIAL_COVERAGE")
            .expect("claude declined delegation and says so")
            .message
            .contains("include_related is off"));
    }

    /// The declared table is what `build_result` computes from, so it is
    /// asserted directly: a provider that grows a parser flips its entry here
    /// and the hydration contract follows without another edit.
    #[test]
    fn declared_coverage_matches_what_each_local_parser_writes() {
        for source in ["claude", "codex", "grok"] {
            assert_eq!(
                crate::discover::declared_evidence_kinds(source),
                FULL_SESSION_KINDS,
                "{source} parses every evidence kind"
            );
            assert!(crate::discover::missing_evidence_kinds(source).is_empty());
        }
        // Cursor parses every kind but delegation: a `Task` block names no
        // child transcript, so no relationship row is ever written.
        assert_eq!(
            crate::discover::declared_evidence_kinds("cursor"),
            &[
                EvidenceKind::History,
                EvidenceKind::SessionEvent,
                EvidenceKind::ToolCall,
                EvidenceKind::FileEdit,
            ],
            "cursor parses everything except delegation"
        );
        assert_eq!(
            crate::discover::missing_evidence_kinds("cursor"),
            vec![EvidenceKind::Relationship],
        );
        assert_eq!(
            crate::discover::declared_evidence_kinds("opencode"),
            &[EvidenceKind::History],
            "opencode parses prompts only"
        );
        assert_eq!(
            crate::discover::missing_evidence_kinds("opencode"),
            vec![
                EvidenceKind::SessionEvent,
                EvidenceKind::ToolCall,
                EvidenceKind::FileEdit,
                EvidenceKind::Relationship,
            ],
        );
        // Relay rows come out of already-ingested history and an unknown
        // source has no adapter at all: neither declares anything.
        assert!(crate::discover::declared_evidence_kinds("relay").is_empty());
        assert!(crate::discover::declared_evidence_kinds("not-a-provider").is_empty());
        assert_eq!(
            crate::discover::missing_evidence_kinds("relay"),
            FULL_SESSION_KINDS.to_vec()
        );
    }

    #[test]
    fn identity_mismatch_rolls_back_catalog_upgrade() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join(".claude/projects/app/wrong.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            "{\"sessionId\":\"different\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"nope\"}}\n",
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "selected", Some(&transcript));
        drop(conn);
        let error = hydrate_session_at_with_home(&db, &options("claude", "selected"), dir.path())
            .unwrap_err();
        assert!(format!("{error:#}").starts_with("SESSION_SOURCE_MISMATCH:"));
        let conn = open_db(&db).unwrap();
        let state: String = conn
            .query_row(
                "SELECT discovery_state FROM sessions WHERE source='claude' AND session_id='selected'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "shallow");
        let checkpoints: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_hydration_checkpoints",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(checkpoints, 0);
    }

    #[test]
    fn opencode_hydration_queries_only_the_selected_session() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join(".local/share/opencode/opencode.db");
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        let src = Connection::open(&source).unwrap();
        src.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER); \
             CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT); \
             CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT); \
             INSERT INTO session VALUES ('selected', '/work/selected', 1), ('unrelated', '/work/other', 3); \
             INSERT INTO message VALUES ('m1', 'selected', 1, '{\"role\":\"user\"}'), ('m2', 'unrelated', 3, '{\"role\":\"user\"}'); \
             INSERT INTO part VALUES ('p1', 'm1', 'selected', 2, '{\"type\":\"text\",\"text\":\"selected prompt\"}'), ('p2', 'm2', 'unrelated', 4, '{\"type\":\"text\",\"text\":\"must not ingest\"}');",
        )
        .unwrap();
        drop(src);
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        let env = DiscoveryEnv::with_all_roots(
            &conn,
            dir.path().into(),
            dir.path().join(".claude"),
            dir.path().join(".codex"),
            dir.path().join(".grok"),
            source.clone(),
        );
        crate::discover::discover_sessions_with_env(
            &env,
            &DiscoverOptions {
                scope: SessionScope::Local,
                sources: vec!["opencode".into()],
                limit: None,
            },
            |_| {},
        )
        .unwrap();
        assert_eq!(
            observations::list(&conn, "opencode", "selected").unwrap()[0]
                .raw_locator
                .as_deref(),
            Some("selected")
        );

        drop(conn);
        let result =
            hydrate_session_at_with_home(&db, &options("opencode", "selected"), dir.path())
                .unwrap();
        assert_eq!(result.evidence.prompts, 1);
        let conn = open_db(&db).unwrap();
        let unrelated: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source='opencode' AND session_id='unrelated'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unrelated, 0);
    }

    #[test]
    fn opencode_hydration_rejects_a_different_catalog_store() {
        let dir = tempfile::tempdir().unwrap();
        let configured = dir.path().join(".local/share/opencode/opencode.db");
        let catalog_store = dir.path().join("other-opencode.db");
        fs::create_dir_all(configured.parent().unwrap()).unwrap();
        for path in [&configured, &catalog_store] {
            let conn = Connection::open(path).unwrap();
            conn.execute_batch(
                "CREATE TABLE session (id TEXT PRIMARY KEY, time_created INTEGER); \
                 CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT); \
                 CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT); \
                 INSERT INTO session VALUES ('selected', 1);",
            )
            .unwrap();
        }
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "opencode", "selected", Some(&catalog_store));
        drop(conn);

        let error = hydrate_session_at_with_home(&db, &options("opencode", "selected"), dir.path())
            .unwrap_err();
        assert!(format!("{error:#}").starts_with("SESSION_SOURCE_MISMATCH:"));
    }

    #[test]
    fn codex_related_threads_keep_identity_and_never_become_human_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join(".codex/sessions/2026/08/31");
        fs::create_dir_all(&day).unwrap();
        let root = day.join("rollout-root.jsonl");
        let child = day.join("rollout-child.jsonl");
        let child_parent_thread = day.join("rollout-child-parent-thread.jsonl");
        let child_thread_spawn = day.join("rollout-child-thread-spawn.jsonl");
        let grandchild = day.join("rollout-grandchild.jsonl");
        fs::write(
            &root,
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"root\",\"cwd\":\"/work/app\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"root prompt\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:02Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"root answer\"}}\n",
            ),
        )
        .unwrap();
        fs::write(
            &grandchild,
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:12Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"grandchild\",\"cwd\":\"/work/app\",\"source\":{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"child-thread-spawn\",\"depth\":2}}}}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:13Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"nested task\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:14Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"nested answer\"}}\n",
            ),
        )
        .unwrap();
        fs::write(
            &child_parent_thread,
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:06Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"child-parent-thread\",\"parent_thread_id\":\"root\",\"cwd\":\"/work/app\",\"source\":{\"subagent\":{\"other\":\"guardian\"}}}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:07Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"guardian task\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:08Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"guardian answer\"}}\n",
            ),
        )
        .unwrap();
        fs::write(
            &child_thread_spawn,
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:09Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"child-thread-spawn\",\"cwd\":\"/work/app\",\"source\":{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"root\",\"depth\":1}}}}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:10Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"spawned task\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:11Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"spawned answer\"}}\n",
            ),
        )
        .unwrap();
        fs::write(
            &child,
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:03Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"child\",\"session_id\":\"root\",\"parent_thread_id\":\"root\",\"cwd\":\"/work/app\",\"thread_source\":\"subagent\",\"source\":{\"subagent\":{\"other\":\"guardian\"}}}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:04Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"delegated task\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:05Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"child answer\"}}\n",
            ),
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "root", Some(&root));
        drop(conn);

        let mut request = options("codex", "root");
        request.include_related = false;
        let root_only = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert!(root_only.related_session_ids.is_empty());

        request.include_related = true;
        let with_child = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert_eq!(with_child.status, "updated");
        assert_eq!(
            with_child.related_session_ids,
            vec![
                "child",
                "child-parent-thread",
                "child-thread-spawn",
                "grandchild"
            ]
        );
        let conn = open_db(&db).unwrap();
        let child_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source='codex' AND session_id != 'root'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(child_events >= 8);
        let child_prompts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source='codex' AND session_id != 'root'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(child_prompts, 0);
        let child_catalog: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE source='codex' AND session_id != 'root'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(child_catalog, 0);
        let relationship: (String, String, String, Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT identity_status, evidence_kind, evidence_locator, child_agent_type, spawned_at_ms \
                 FROM session_relationships WHERE source='codex' AND child_session_id='child'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(relationship.0, "observed");
        assert_eq!(relationship.1, "codex_session_meta");
        assert_eq!(relationship.2, child.to_string_lossy());
        assert_eq!(relationship.3.as_deref(), Some("guardian"));
        assert!(relationship.4.is_some());
        let grandchild_parent: String = conn
            .query_row(
                "SELECT parent_session_id FROM session_relationships \
                 WHERE source='codex' AND child_session_id='grandchild'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(grandchild_parent, "child-thread-spawn");
    }

    #[test]
    fn codex_child_prompts_are_not_human_history() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join(".codex/sessions/2026/08/31");
        fs::create_dir_all(&day).unwrap();
        let root = day.join("rollout-root.jsonl");
        fs::write(
            &root,
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"root\",\"cwd\":\"/work/app\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"root prompt\"}}\n",
            ),
        )
        .unwrap();
        fs::write(
            day.join("rollout-child.jsonl"),
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:03Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"child\",\"session_id\":\"root\",\"cwd\":\"/work/app\",\"thread_source\":\"subagent\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:04Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"delegated task\"}}\n",
            ),
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "root", Some(&root));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        let counts: (i64, i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM history WHERE source='codex' AND session_id='child'), \
                   (SELECT COUNT(*) FROM sessions WHERE source='codex' AND session_id='child'), \
                   (SELECT COUNT(*) FROM history WHERE source='codex' AND session_id='root')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(counts, (0, 0, 1));
    }

    /// A parent transcript plus one subagent transcript under
    /// `<sessionId>/subagents/`, in the layout the provider actually writes.
    fn claude_parent_with_subagent(
        home: &Path,
        agent_records: &str,
        agent_meta: Option<&str>,
        stem: &str,
    ) -> PathBuf {
        let transcript = home.join(".claude/projects/app/session-1.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            concat!(
                "{\"sessionId\":\"session-1\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"first prompt\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
                "{\"sessionId\":\"session-1\",\"uuid\":\"a1\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"Agent\",\"input\":{\"prompt\":\"plan it\"}}]},\"timestamp\":\"2026-08-31T10:00:01Z\"}\n",
            ),
        )
        .unwrap();
        let subagents = home.join(".claude/projects/app/session-1/subagents");
        fs::create_dir_all(&subagents).unwrap();
        fs::write(subagents.join(format!("{stem}.jsonl")), agent_records).unwrap();
        if let Some(meta) = agent_meta {
            fs::write(subagents.join(format!("{stem}.meta.json")), meta).unwrap();
        }
        transcript
    }

    const CLAUDE_AGENT_RECORDS: &str = concat!(
        "{\"sessionId\":\"session-1\",\"agentId\":\"abc\",\"isSidechain\":true,\"uuid\":\"side-u\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"delegated instruction\"},\"timestamp\":\"2026-08-31T10:00:02Z\"}\n",
        "{\"sessionId\":\"session-1\",\"agentId\":\"abc\",\"isSidechain\":true,\"uuid\":\"side-a\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"child result\"},\"timestamp\":\"2026-08-31T10:00:03Z\"}\n",
    );
    const CLAUDE_AGENT_META: &str = "{\"agentType\":\"Plan\",\"description\":\"plan the work\",\"toolUseId\":\"toolu_1\",\"spawnDepth\":1,\"model\":\"opus\"}";

    struct LinkedChild {
        identity_status: String,
        agent_type: Option<String>,
        agent_name: Option<String>,
        model: Option<String>,
        spawn_depth: Option<i64>,
        evidence_ref: Option<String>,
        has_events: bool,
    }

    #[test]
    fn claude_subagent_with_agent_id_is_a_linked_child() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = claude_parent_with_subagent(
            dir.path(),
            CLAUDE_AGENT_RECORDS,
            Some(CLAUDE_AGENT_META),
            "agent-abc",
        );
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert_eq!(result.related_session_ids, vec!["abc"]);

        let conn = open_db(&db).unwrap();
        let row = conn
            .query_row(
                "SELECT identity_status, child_agent_type, child_agent_name, child_model, \
                        spawn_depth, evidence_ref, child_has_events \
                 FROM session_relationships WHERE source='claude' AND child_session_id='abc'",
                [],
                |row| {
                    Ok(LinkedChild {
                        identity_status: row.get(0)?,
                        agent_type: row.get(1)?,
                        agent_name: row.get(2)?,
                        model: row.get(3)?,
                        spawn_depth: row.get(4)?,
                        evidence_ref: row.get(5)?,
                        has_events: row.get(6)?,
                    })
                },
            )
            .unwrap();
        assert_eq!(row.identity_status, "observed");
        assert_eq!(row.agent_type.as_deref(), Some("Plan"));
        assert_eq!(row.agent_name.as_deref(), Some("plan the work"));
        assert_eq!(row.model.as_deref(), Some("opus"));
        assert_eq!(row.spawn_depth, Some(1));
        assert_eq!(row.evidence_ref.as_deref(), Some("toolu_1"));
        assert!(row.has_events);

        // The child's own output is addressable under the child, its
        // delegated instruction is nobody's human prompt, and a delegated
        // thread never becomes a session of its own.
        let counts: (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='abc'), \
                   (SELECT COUNT(*) FROM history WHERE source='claude' AND session_id='abc'), \
                   (SELECT COUNT(*) FROM sessions WHERE source='claude' AND session_id='abc'), \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='session-1' AND event_uid LIKE 'side-%')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(counts, (1, 0, 0, 0));
    }

    #[test]
    fn claude_sidechain_without_agent_id_stays_unlinked() {
        let dir = tempfile::tempdir().unwrap();
        let records = concat!(
            "{\"sessionId\":\"session-1\",\"isSidechain\":true,\"uuid\":\"side-u\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"delegated instruction\"},\"timestamp\":\"2026-08-31T10:00:02Z\"}\n",
            "{\"sessionId\":\"session-1\",\"isSidechain\":true,\"uuid\":\"side-a\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"side result\"},\"timestamp\":\"2026-08-31T10:00:03Z\"}\n",
        );
        let transcript = claude_parent_with_subagent(dir.path(), records, None, "agent-child");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert!(result.related_session_ids.is_empty());
        assert!(result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "RELATIONSHIP_UNLINKED_CHILD"));

        let conn = open_db(&db).unwrap();
        let row: (String, i64, i64) = conn
            .query_row(
                "SELECT identity_status, child_session_id IS NULL, child_has_events \
                 FROM session_relationships WHERE source='claude' AND parent_session_id='session-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(row, ("unlinked".to_string(), 1, 0));
        // The subagent's assistant output stays on the parent, where it is
        // the only place it can be addressed.
        let parent_side_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events \
                 WHERE source='claude' AND session_id='session-1' AND event_uid = 'side-a:0'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(parent_side_events, 1);
    }

    #[test]
    fn claude_child_identity_is_never_taken_from_the_filename() {
        let dir = tempfile::tempdir().unwrap();
        let records = "{\"sessionId\":\"session-1\",\"isSidechain\":true,\"uuid\":\"side-a\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"side result\"},\"timestamp\":\"2026-08-31T10:00:03Z\"}\n";
        let transcript = claude_parent_with_subagent(dir.path(), records, None, "agent-deadbeef");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        let named_after_the_file: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships WHERE child_session_id = 'deadbeef'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(named_after_the_file, 0);
        let events_under_the_file_stem: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = 'deadbeef'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(events_under_the_file_stem, 0);
    }

    #[test]
    fn reparse_moves_parent_attributed_sidechain_events_to_the_child() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = claude_parent_with_subagent(
            dir.path(),
            CLAUDE_AGENT_RECORDS,
            Some(CLAUDE_AGENT_META),
            "agent-abc",
        );
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        // What the previous parser version left behind: the subagent's
        // assistant output attributed to the parent session.
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 'session-1', 1, 'assistant', 'text', 'child result', 'side-a:0')",
            [],
        )
        .unwrap();
        drop(conn);

        hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        let placement: (i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='session-1' AND event_uid='side-a:0'), \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='abc' AND event_uid='side-a:0')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(placement, (0, 1));
    }

    #[test]
    fn reparse_moves_parent_attributed_tool_calls_and_file_edits_to_the_child() {
        let dir = tempfile::tempdir().unwrap();
        // The child's own tool use: an earlier parser version derived a tool
        // call and a file edit from it under the parent's identity.
        let records = concat!(
            r#"{"sessionId":"session-1","agentId":"abc","isSidechain":true,"uuid":"side-a","cwd":"/work/app","type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_child","name":"Edit","input":{"file_path":"/work/app/lib.rs"}}]},"timestamp":"2026-08-31T10:00:03Z"}"#,
            "\n",
        );
        let transcript = claude_parent_with_subagent(dir.path(), records, None, "agent-abc");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 'session-1', 1, 'assistant', 'tool_use', 'Edit /work/app/lib.rs', 'side-a:0')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tool_calls \
             (source, session_id, message_id, tool_use_id, name, target, args_json, ts_ms) \
             VALUES ('claude', 'session-1', 'side-a', 'toolu_child', 'Edit', '/work/app/lib.rs', '{}', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO file_edits \
             (source, session_id, message_id, tool_use_id, file_path, tool_name, ts_ms) \
             VALUES ('claude', 'session-1', 'side-a', 'toolu_child', '/work/app/lib.rs', 'Edit', 1)",
            [],
        )
        .unwrap();
        drop(conn);

        hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        // A delegated action belongs to the thread that took it: the parent
        // must not keep exposing the child's tool call or file edit as its own.
        let placement: (i64, i64, i64, i64, i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='session-1' AND event_uid='side-a:0'), \
                   (SELECT COUNT(*) FROM tool_calls WHERE source='claude' AND session_id='session-1' AND tool_use_id='toolu_child'), \
                   (SELECT COUNT(*) FROM file_edits WHERE source='claude' AND session_id='session-1' AND tool_use_id='toolu_child'), \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='abc' AND event_uid='side-a:0'), \
                   (SELECT COUNT(*) FROM tool_calls WHERE source='claude' AND session_id='abc' AND tool_use_id='toolu_child'), \
                   (SELECT COUNT(*) FROM file_edits WHERE source='claude' AND session_id='abc' AND tool_use_id='toolu_child')",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(placement, (0, 0, 0, 1, 1, 1));
    }

    #[test]
    fn a_child_named_only_by_a_later_record_still_links() {
        let dir = tempfile::tempdir().unwrap();
        // A mixed transcript: the provider started naming the child partway
        // through, so the identity is not on the first record.
        let records = concat!(
            r#"{"sessionId":"session-1","isSidechain":true,"uuid":"side-u","type":"user","message":{"role":"user","content":"delegated instruction"},"timestamp":"2026-08-31T10:00:02Z"}"#,
            "\n",
            r#"{"sessionId":"session-1","agentId":"abc","isSidechain":true,"uuid":"side-a","type":"assistant","message":{"role":"assistant","content":"child result"},"timestamp":"2026-08-31T10:00:03Z"}"#,
            "\n",
        );
        let transcript = claude_parent_with_subagent(dir.path(), records, None, "agent-abc");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert_eq!(result.related_session_ids, vec!["abc"]);
        assert!(!result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "RELATIONSHIP_UNLINKED_CHILD"));
        let conn = open_db(&db).unwrap();
        let placement: (i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='abc'), \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='session-1' AND event_uid='side-a:0')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(placement, (1, 0));
    }

    #[test]
    fn a_changed_metadata_sidecar_refreshes_the_child_description() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = claude_parent_with_subagent(
            dir.path(),
            CLAUDE_AGENT_RECORDS,
            Some(CLAUDE_AGENT_META),
            "agent-abc",
        );
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);
        hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();

        // The transcript is untouched; only what describes the child changed.
        fs::write(
            dir.path()
                .join(".claude/projects/app/session-1/subagents/agent-abc.meta.json"),
            "{\"agentType\":\"Explore\",\"description\":\"explore the code\",\"toolUseId\":\"toolu_1\",\"spawnDepth\":2,\"model\":\"haiku\"}",
        )
        .unwrap();
        let refreshed =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert_eq!(refreshed.status, "updated");

        let conn = open_db(&db).unwrap();
        let row: (Option<String>, Option<String>, Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT child_agent_type, child_agent_name, child_model, spawn_depth \
                 FROM session_relationships WHERE source='claude' AND child_session_id='abc'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (
                Some("Explore".to_string()),
                Some("explore the code".to_string()),
                Some("haiku".to_string()),
                Some(2),
            )
        );
    }

    #[test]
    fn hydration_metrics_count_every_file_the_run_read() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = claude_parent_with_subagent(
            dir.path(),
            CLAUDE_AGENT_RECORDS,
            Some(CLAUDE_AGENT_META),
            "agent-abc",
        );
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        let subagents = dir.path().join(".claude/projects/app/session-1/subagents");
        // The metadata sidecar is evidence the run acquired like any other, so
        // its bytes belong in what the metrics report was read.
        let expected: i64 = [
            transcript,
            subagents.join("agent-abc.jsonl"),
            subagents.join("agent-abc.meta.json"),
        ]
        .iter()
        .map(|path| fs::metadata(path).unwrap().len() as i64)
        .sum();
        let metrics = result
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "HYDRATION_METRICS")
            .unwrap();
        assert_eq!(metrics.source_bytes, Some(expected));
    }

    #[test]
    fn a_later_full_sync_leaves_subagent_events_on_the_child() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = claude_parent_with_subagent(
            dir.path(),
            CLAUDE_AGENT_RECORDS,
            Some(CLAUDE_AGENT_META),
            "agent-abc",
        );
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);
        hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();

        let conn = open_db(&db).unwrap();
        let projects = dir.path().join(".claude/projects");
        // A sidecar walked by the full sync is not a session: it must not
        // pull the child's output back onto the parent, take over the
        // parent's catalog locator, or register itself.
        for _ in 0..2 {
            sync_claude_session_metadata(&conn, &mut Map::new(), &projects).unwrap();
            let placement: (i64, i64, i64, String) = conn
                .query_row(
                    "SELECT \
                       (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='abc'), \
                       (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='session-1' AND event_uid='side-a:0'), \
                       (SELECT COUNT(*) FROM sessions WHERE source='claude' AND session_id='abc'), \
                       (SELECT raw_path FROM sessions WHERE source='claude' AND session_id='session-1')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();
            assert_eq!(
                placement,
                (1, 0, 0, transcript.to_string_lossy().to_string())
            );
        }
    }

    #[test]
    fn healing_a_record_never_matches_another_id_by_wildcard() {
        let dir = tempfile::tempdir().unwrap();
        let records = "{\"sessionId\":\"session-1\",\"agentId\":\"abc\",\"isSidechain\":true,\"uuid\":\"side_a%1\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"child result\"},\"timestamp\":\"2026-08-31T10:00:03Z\"}\n";
        let transcript = claude_parent_with_subagent(dir.path(), records, None, "agent-abc");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        // The stale row this record left on the parent, plus an unrelated
        // event whose uid a `LIKE 'side_a%1:%'` pattern would also match.
        for uid in ["side_a%1:0", "sideXaY1:0"] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 'session-1', 1, 'assistant', 'text', 'stale', ?)",
                params![uid],
            )
            .unwrap();
        }
        drop(conn);

        hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        let placement: (i64, i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM session_events WHERE session_id='session-1' AND event_uid='side_a%1:0'), \
                   (SELECT COUNT(*) FROM session_events WHERE session_id='session-1' AND event_uid='sideXaY1:0'), \
                   (SELECT COUNT(*) FROM session_events WHERE session_id='abc' AND event_uid='side_a%1:0')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(placement, (0, 1, 1));
    }

    #[test]
    fn a_record_without_a_uuid_is_healed_under_its_derived_id() {
        let dir = tempfile::tempdir().unwrap();
        let records = "{\"sessionId\":\"session-1\",\"agentId\":\"abc\",\"isSidechain\":true,\"type\":\"assistant\",\"message\":{\"id\":\"msg_1\",\"role\":\"assistant\",\"content\":\"child result\"},\"timestamp\":\"2026-08-31T10:00:03Z\"}\n";
        let transcript = claude_parent_with_subagent(dir.path(), records, None, "agent-abc");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        // Insertion falls back to the message id when a record carries no
        // uuid, so healing has to reach the same identity.
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 'session-1', 1, 'assistant', 'text', 'child result', 'msg_1:0')",
            [],
        )
        .unwrap();
        drop(conn);

        hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        let placement: (i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM session_events WHERE session_id='session-1' AND event_uid='msg_1:0'), \
                   (SELECT COUNT(*) FROM session_events WHERE session_id='abc' AND event_uid='msg_1:0')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(placement, (0, 1));
    }

    #[test]
    fn an_upgraded_provider_retires_the_unlinked_row_it_supersedes() {
        let dir = tempfile::tempdir().unwrap();
        // First the provider version that records the sidechain without
        // naming the child.
        let unnamed = "{\"sessionId\":\"session-1\",\"isSidechain\":true,\"uuid\":\"side-a\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"child result\"},\"timestamp\":\"2026-08-31T10:00:03Z\"}\n";
        let transcript = claude_parent_with_subagent(dir.path(), unnamed, None, "agent-abc");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);
        hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();

        // Then the same file, rewritten by a version that does name it.
        let subagents = dir.path().join(".claude/projects/app/session-1/subagents");
        fs::write(subagents.join("agent-abc.jsonl"), CLAUDE_AGENT_RECORDS).unwrap();
        fs::write(subagents.join("agent-abc.meta.json"), CLAUDE_AGENT_META).unwrap();
        let result =
            hydrate_session_at_with_home(&db, &options("claude", "session-1"), dir.path()).unwrap();
        assert_eq!(result.related_session_ids, vec!["abc"]);
        assert!(!result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "RELATIONSHIP_UNLINKED_CHILD"));

        let conn = open_db(&db).unwrap();
        let rows: Vec<(Option<String>, String)> = conn
            .prepare(
                "SELECT child_session_id, identity_status FROM session_relationships \
                 WHERE source='claude' AND parent_session_id='session-1'",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![(Some("abc".to_string()), "observed".to_string())]
        );
    }

    #[test]
    fn root_only_hydration_then_related_hydration() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = claude_parent_with_subagent(
            dir.path(),
            CLAUDE_AGENT_RECORDS,
            Some(CLAUDE_AGENT_META),
            "agent-abc",
        );
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-1", Some(&transcript));
        drop(conn);

        let mut request = options("claude", "session-1");
        request.include_related = false;
        let root_only = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert!(root_only.related_session_ids.is_empty());
        let root_prompts = root_only.evidence.prompts;
        let conn = open_db(&db).unwrap();
        let untouched: (i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM session_relationships), \
                   (SELECT COUNT(*) FROM session_events WHERE session_id='abc')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(untouched, (0, 0));
        drop(conn);

        request.include_related = true;
        let related = hydrate_session_at_with_home(&db, &request, dir.path()).unwrap();
        assert_eq!(related.related_session_ids, vec!["abc"]);
        assert_eq!(related.evidence.prompts, root_prompts);
        let conn = open_db(&db).unwrap();
        let now: (i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM session_relationships), \
                   (SELECT COUNT(*) FROM session_events WHERE session_id='abc')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(now, (1, 1));
    }

    #[test]
    fn standalone_codex_guardian_source_marker_hydrates_without_a_relation() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join(".codex/sessions/2026/05/05");
        fs::create_dir_all(&day).unwrap();
        let guardian = day.join("rollout-guardian.jsonl");
        fs::write(
            &guardian,
            concat!(
                "{\"timestamp\":\"2026-05-05T19:13:27Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"guardian\",\"cwd\":\"/work/api\",\"source\":{\"subagent\":{\"other\":\"guardian\"}}}}\n",
                "{\"timestamp\":\"2026-05-05T19:13:28Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"guardian prompt\"}}\n",
                "{\"timestamp\":\"2026-05-05T19:13:29Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"guardian answer\"}}\n",
            ),
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "guardian", Some(&guardian));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("codex", "guardian"), dir.path()).unwrap();
        assert_eq!(first.status, "hydrated");
        assert_eq!(first.related_session_ids, Vec::<String>::new());
        assert_eq!(first.evidence.prompts, 1);
        assert_eq!(first.evidence.events, 2);
        assert_eq!(first.evidence.tool_calls, 0);

        let conn = open_db(&db).unwrap();
        let raw_path: String = conn
            .query_row(
                "SELECT raw_path FROM sessions WHERE source='codex' AND session_id='guardian'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(raw_path, guardian.to_string_lossy());
        let relationships: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships WHERE source='codex'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(relationships, 0);
        let second =
            hydrate_session_at_with_home(&db, &options("codex", "guardian"), dir.path()).unwrap();
        assert_eq!(second.status, "unchanged");
        assert_eq!(second.evidence.prompts, 1);
        assert_eq!(second.evidence.events, 2);
    }

    #[test]
    fn thousands_of_unrelated_catalog_rows_do_not_expand_hydration_work() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join(".claude/projects/app/selected.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            "{\"sessionId\":\"selected\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"bounded\"},\"timestamp\":1}\n",
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        conn.execute_batch(
            "WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM seq WHERE x < 5000) \
             INSERT INTO sessions (source, session_id, discovery_state) \
             SELECT 'codex', 'unrelated-' || x, 'shallow' FROM seq;",
        )
        .unwrap();
        catalog_row(&conn, "claude", "selected", Some(&transcript));
        drop(conn);
        let result =
            hydrate_session_at_with_home(&db, &options("claude", "selected"), dir.path()).unwrap();
        assert_eq!(result.evidence.prompts, 1);
        assert_eq!(result.diagnostics[0].records_parsed, Some(1));
        let conn = open_db(&db).unwrap();
        let full: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE discovery_state='full'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(full, 1);
    }

    #[test]
    fn simultaneous_hydration_remains_deduplicated() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join(".claude/projects/app/concurrent.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            "{\"sessionId\":\"concurrent\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"once\"},\"timestamp\":1}\n",
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "concurrent", Some(&transcript));
        drop(conn);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let handles = (0..2)
            .map(|_| {
                let db = db.clone();
                let home = dir.path().to_path_buf();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    hydrate_session_at_with_home(&db, &options("claude", "concurrent"), &home)
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }
        let conn = open_db(&db).unwrap();
        let counts: (i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM history WHERE source='claude' AND session_id='concurrent'), \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='concurrent')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (1, 1));
    }

    fn remote_catalog_row(conn: &Connection, source: &str, session_id: &str) {
        conn.execute(
            "INSERT INTO sessions (source, session_id, first_prompt, discovery_state) VALUES (?, ?, 'same title', 'shallow')",
            params![source, session_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_presences (source, session_id, location, raw_locator, source_stamp, discovery_state) \
             VALUES (?, ?, 'remote', ?, 'remote-listing', 'shallow')",
            params![source, session_id, session_id],
        )
        .unwrap();
    }

    #[test]
    fn remote_capability_limit_is_a_structured_supported_result() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        remote_catalog_row(&conn, "cursor", "remote-cursor");
        record_relationship(
            &conn,
            &ObservedRelationship {
                source: "cursor",
                parent_session_id: "remote-cursor",
                child_session_id: Some("related-child"),
                relationship: "delegated",
                child_agent_type: None,
                child_agent_name: None,
                child_model: None,
                spawn_depth: None,
                evidence_kind: "test",
                evidence_locator: None,
                evidence_ref: None,
                child_has_events: true,
                spawned_at_ms: None,
                ..ObservedRelationship::default()
            },
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('cursor', 'related-child', 1, 'assistant', 'text', 'child output', 'child-event')",
            [],
        )
        .unwrap();
        drop(conn);
        let result = remote_limited_result(
            &mut open_db(&db).unwrap(),
            &HydrateSessionOptions {
                source: "cursor".into(),
                session_id: "remote-cursor".into(),
                scope: SessionScope::Remote,
                include_related: true,
            },
            "CONNECTOR_NOT_CONFIGURED",
            "selected source provides discovery only".into(),
            Instant::now(),
        )
        .unwrap();
        assert_eq!(result.status, "capability_limited");
        assert_eq!(result.capability, "shallow_only");
        assert_eq!(result.discovery_state, "shallow");
        assert_eq!(result.presence, "remote");
        assert_eq!(result.related_session_ids, vec!["related-child"]);
        assert_eq!(result.evidence.events, 1);
        assert_eq!(result.diagnostics[0].code, "CONNECTOR_NOT_CONFIGURED");
    }

    /// A complete remote snapshot is a replacement, not an addition: the rows
    /// it no longer contains are deleted before the new ones land. A marker is
    /// evidence exactly like an event, so one left behind tells a caller
    /// something is still there that the provider has removed.
    #[test]
    fn a_full_remote_snapshot_removes_markers_it_no_longer_contains() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let mut conn = open_db(&db).unwrap();
        remote_catalog_row(&conn, "claude", "session_01marked");
        let options = HydrateSessionOptions {
            source: "claude".into(),
            session_id: "session_01marked".into(),
            scope: SessionScope::Remote,
            include_related: true,
        };
        let prompt = serde_json::json!({
            "sessionId": "session_01marked", "uuid": "u1", "type": "user", "timestamp": 1,
            "message": {"role": "user", "content": "remote prompt"}
        });
        let boundary = serde_json::json!({
            "sessionId": "session_01marked", "uuid": "s1", "type": "system",
            "subtype": "compact_boundary", "timestamp": 2
        });

        let marker_kinds = |conn: &Connection| -> Vec<String> {
            crate::session_markers_page(conn, "claude", "session_01marked", 100, None)
                .unwrap()
                .markers
                .into_iter()
                .map(|marker| marker.kind)
                .collect()
        };

        hydrate_remote_claude(
            &mut conn,
            &options,
            vec![prompt.clone(), boundary],
            "teleport:marked".into(),
            100,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(
            marker_kinds(&conn),
            vec!["compaction_boundary".to_string()],
            "the first snapshot records the boundary"
        );

        // The provider no longer sends the boundary record.
        hydrate_remote_claude(
            &mut conn,
            &options,
            vec![prompt],
            "teleport:unmarked".into(),
            50,
            Instant::now(),
        )
        .unwrap();
        assert!(
            marker_kinds(&conn).is_empty(),
            "a marker the provider removed must not survive a full snapshot: {:?}",
            marker_kinds(&conn)
        );
    }

    #[test]
    fn claude_remote_full_evidence_is_atomic_idempotent_and_preserves_remote_presence() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let mut conn = open_db(&db).unwrap();
        remote_catalog_row(&conn, "claude", "session_01full");
        let options = HydrateSessionOptions {
            source: "claude".into(),
            session_id: "session_01full".into(),
            scope: SessionScope::Remote,
            include_related: true,
        };
        let records = vec![
            serde_json::json!({
                "session_id": "session_01full", "uuid": "u1", "type": "user", "timestamp": 1,
                "message": {"role": "user", "content": "remote prompt"}
            }),
            serde_json::json!({
                "sessionId": "session_01full", "uuid": "a1", "type": "assistant", "timestamp": 2,
                "message": {"role": "assistant", "model": "claude-test", "usage": {"input_tokens": 2},
                    "content": [{"type": "tool_use", "id": "tool-1", "name": "Read", "input": {"file_path": "/work/a"}}]}
            }),
        ];
        let first = hydrate_remote_claude(
            &mut conn,
            &options,
            records.clone(),
            "teleport:one".into(),
            100,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(first.status, "hydrated");
        assert_eq!(first.capability, "full");
        assert_eq!(first.evidence.prompts, 1);
        assert_eq!(first.evidence.events, 2);
        assert_eq!(first.evidence.tool_calls, 1);
        let presences = conn
            .prepare("SELECT location, discovery_state FROM session_presences WHERE source='claude' AND session_id='session_01full' ORDER BY location")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(presences, vec![("remote".into(), "full".into())]);

        let repeated = hydrate_remote_claude(
            &mut conn,
            &options,
            records.clone(),
            "teleport:one".into(),
            100,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(repeated.status, "unchanged");
        assert_eq!(repeated.evidence.events, 2);

        conn.execute(
            "UPDATE session_hydration_checkpoints SET parser_version = 0 \
             WHERE source = 'claude' AND session_id = 'session_01full' AND location = 'remote'",
            [],
        )
        .unwrap();
        let reparsed = hydrate_remote_claude(
            &mut conn,
            &options,
            records.clone(),
            "teleport:one".into(),
            100,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(reparsed.status, "updated");

        let reduced = hydrate_remote_claude(
            &mut conn,
            &options,
            vec![records[0].clone()],
            "teleport:two".into(),
            50,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(reduced.evidence.events, 1);
        assert_eq!(reduced.evidence.tool_calls, 0);

        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_events", [], |row| row.get(0))
            .unwrap();
        let malformed = vec![serde_json::json!({
            "sessionId": "session_different", "uuid": "bad", "type": "user", "timestamp": 3,
            "message": {"role": "user", "content": "must roll back"}
        })];
        assert!(hydrate_remote_claude(
            &mut conn,
            &options,
            malformed,
            "teleport:bad".into(),
            20,
            Instant::now(),
        )
        .is_err());
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn legacy_codex_diff_is_not_duplicated_or_claimed_by_a_new_observation() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = open_db(&dir.path().join("history.db")).unwrap();
        remote_catalog_row(&conn, "codex", "task_e_123");
        conn.execute("INSERT INTO file_edits(source,session_id,tool_use_id,file_path,tool_name,structured_patch_json) VALUES('codex','task_e_123','remote-diff:0','a.txt','codex cloud diff','legacy patch')",[]).unwrap();
        let observed = SessionObservation {
            key: ObservationKey {
                source: "codex".into(),
                session_id: "task_e_123".into(),
                location: SessionLocation::Remote,
                connector_id: "codex-cloud".into(),
                connector_instance: "default".into(),
            },
            raw_locator: Some("task_e_123".into()),
            source_stamp: Some("listing".into()),
            discovery_state: "shallow".into(),
            access_state: "available".into(),
            updated_ms: 1,
        };
        observations::upsert(&conn, &observed).unwrap();
        let options = HydrateSessionOptions {
            source: "codex".into(),
            session_id: "task_e_123".into(),
            scope: SessionScope::Remote,
            include_related: false,
        };
        let diff =
            "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1,2 @@\n old\n+fresh\n";
        hydrate_remote_codex_diff_observed(
            &mut conn,
            &options,
            diff,
            "new".into(),
            100,
            Instant::now(),
            Some(&observed),
        )
        .unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM file_edits", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT structured_patch_json FROM file_edits", [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
            "legacy patch"
        );
        assert_eq!(
            observations::evidence(&conn, &observed.key)
                .unwrap()
                .unwrap()["diff"],
            diff
        );
        assert_eq!(
            observations::checkpoint(&conn, &observed.key)
                .unwrap()
                .unwrap()
                .source_stamp
                .as_deref(),
            Some("new")
        );
    }

    #[test]
    fn same_connector_instance_diff_locations_do_not_delete_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = open_db(&dir.path().join("history.db")).unwrap();
        remote_catalog_row(&conn, "codex", "task_e_123");
        let options = HydrateSessionOptions {
            source: "codex".into(),
            session_id: "task_e_123".into(),
            scope: SessionScope::Remote,
            include_related: false,
        };
        let diff =
            "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1,2 @@\n old\n+new\n";
        let observation = |location| SessionObservation {
            key: ObservationKey {
                source: "codex".into(),
                session_id: "task_e_123".into(),
                location,
                connector_id: "same".into(),
                connector_instance: "same".into(),
            },
            raw_locator: Some("diff".into()),
            source_stamp: Some("1".into()),
            discovery_state: "shallow".into(),
            access_state: "available".into(),
            updated_ms: 1,
        };
        for location in [SessionLocation::Local, SessionLocation::Remote] {
            let observed = observation(location);
            observations::upsert(&conn, &observed).unwrap();
            hydrate_remote_codex_diff_observed(
                &mut conn,
                &options,
                diff,
                "1".into(),
                100,
                Instant::now(),
                Some(&observed),
            )
            .unwrap();
        }
        assert_eq!(
            conn.query_row("SELECT count(*) FROM file_edits", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        hydrate_remote_codex_diff_observed(
            &mut conn,
            &options,
            "",
            "2".into(),
            0,
            Instant::now(),
            Some(&observation(SessionLocation::Remote)),
        )
        .unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM file_edits", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn codex_diff_hydration_is_idempotent_incremental_and_honestly_partial() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let mut conn = open_db(&db).unwrap();
        remote_catalog_row(&conn, "codex", "task_e_123");
        let options = HydrateSessionOptions {
            source: "codex".into(),
            session_id: "task_e_123".into(),
            scope: SessionScope::Remote,
            include_related: true,
        };
        let diff =
            "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1,2 @@\n old\n+new\n";
        let first = hydrate_remote_codex_diff(
            &mut conn,
            &options,
            diff,
            "diff:one".into(),
            diff.len() as i64,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(first.status, "hydrated");
        assert_eq!(first.capability, "partial");
        assert_eq!(first.discovery_state, "shallow");
        assert_eq!(first.evidence.file_edits, 1);
        assert_eq!(first.diagnostics[0].code, "EVIDENCE_PARTIAL");
        // A `partial` capability is only actionable if something names what is
        // absent. The remote and plugin paths used to report the capability
        // and stop there, unlike the local one.
        assert_eq!(first.coverage, vec![EvidenceKind::FileEdit]);
        let partial = first
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "HYDRATION_PARTIAL_COVERAGE")
            .expect("a partial remote snapshot names the kinds it does not cover");
        assert_eq!(
            partial.message,
            "codex evidence covers file_edit; this hydration produces no \
             history, session_event, tool_call, relationship"
        );
        // The acquisition's own diagnostic is kept, not replaced.
        assert!(first
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "EVIDENCE_PARTIAL"));

        let repeated = hydrate_remote_codex_diff(
            &mut conn,
            &options,
            diff,
            "diff:one".into(),
            diff.len() as i64,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(repeated.status, "unchanged");
        assert_eq!(repeated.evidence.file_edits, 1);

        conn.execute(
            "UPDATE session_hydration_checkpoints SET parser_version = 0 \
             WHERE source = 'codex' AND session_id = 'task_e_123' AND location = 'remote'",
            [],
        )
        .unwrap();
        let reparsed = hydrate_remote_codex_diff(
            &mut conn,
            &options,
            diff,
            "diff:one".into(),
            diff.len() as i64,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(reparsed.status, "updated");

        let appended = format!(
            "{diff}diff --git \"a/old name.txt\" \"b/new name.txt\"\n--- \"a/old name.txt\"\n+++ \"b/new name.txt\"\n@@ -0,0 +1 @@\n+added\n"
        );
        let updated = hydrate_remote_codex_diff(
            &mut conn,
            &options,
            &appended,
            "diff:two".into(),
            appended.len() as i64,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(updated.status, "updated");
        assert_eq!(updated.evidence.file_edits, 2);
        let paths = conn
            .prepare("SELECT file_path FROM file_edits WHERE source='codex' AND session_id='task_e_123' ORDER BY file_path")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(paths, vec!["a.txt", "new name.txt"]);

        let limited = remote_limited_result(
            &mut conn,
            &options,
            "PROVIDER_CAPABILITY_LIMITED",
            "no diff".into(),
            Instant::now(),
        )
        .unwrap();
        assert_eq!(limited.evidence.file_edits, 0);
        let checkpoints: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_hydration_checkpoints WHERE source='codex' AND session_id='task_e_123'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(checkpoints, 0);
    }

    #[test]
    fn codex_diff_hydration_rolls_back_partial_file_edits_and_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let mut conn = open_db(&db).unwrap();
        remote_catalog_row(&conn, "codex", "task_e_atomic");
        conn.execute_batch(
            "CREATE TRIGGER reject_second_remote_edit BEFORE INSERT ON file_edits \
             WHEN NEW.file_path = 'b.txt' BEGIN SELECT RAISE(ABORT, 'injected remote edit failure'); END;",
        )
        .unwrap();
        let options = HydrateSessionOptions {
            source: "codex".into(),
            session_id: "task_e_atomic".into(),
            scope: SessionScope::Remote,
            include_related: true,
        };
        let diff = "diff --git a/a.txt b/a.txt\n+one\ndiff --git a/b.txt b/b.txt\n+two\n";
        assert!(hydrate_remote_codex_diff(
            &mut conn,
            &options,
            diff,
            "diff:atomic".into(),
            diff.len() as i64,
            Instant::now(),
        )
        .is_err());
        let counts: (i64, i64) = conn
            .query_row(
                "SELECT (SELECT COUNT(*) FROM file_edits WHERE session_id='task_e_atomic'), \
                        (SELECT COUNT(*) FROM session_hydration_checkpoints WHERE session_id='task_e_atomic')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (0, 0));
    }

    #[test]
    fn deterministic_claude_remote_id_correlates_but_title_similarity_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let transcript = dir.path().join(".claude/projects/app/local.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            "{\"sessionId\":\"local-1\",\"remoteSessionId\":\"session_01remote\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"timestamp\":1,\"message\":{\"role\":\"user\",\"content\":\"same title\"}}\n",
        )
        .unwrap();
        let similar = dir.path().join(".claude/projects/app/similar.jsonl");
        fs::write(
            &similar,
            "{\"sessionId\":\"local-2\",\"uuid\":\"u2\",\"cwd\":\"/work/app\",\"type\":\"user\",\"timestamp\":2,\"message\":{\"role\":\"user\",\"content\":\"same title\"}}\n",
        )
        .unwrap();
        let conn = open_db(&db).unwrap();
        remote_catalog_row(&conn, "claude", "session_01remote");
        catalog_row(&conn, "claude", "local-1", Some(&transcript));
        catalog_row(&conn, "claude", "local-2", Some(&similar));
        drop(conn);
        hydrate_session_at_with_home(&db, &options("claude", "local-1"), dir.path()).unwrap();
        hydrate_session_at_with_home(&db, &options("claude", "local-2"), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        let relationships = conn
            .prepare("SELECT parent_session_id, child_session_id, relationship FROM session_relationships ORDER BY child_session_id")
            .unwrap()
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            relationships,
            vec![(
                "session_01remote".into(),
                "local-1".into(),
                "materialized_local".into()
            )]
        );
    }

    /// Copy one of burn's transcript fixtures into a Claude project directory
    /// under `home`, keeping its original file name — the name is what tells
    /// two branches of one conversation apart.
    fn claude_fixture(home: &Path, name: &str) -> PathBuf {
        let projects = home.join(".claude/projects/app");
        fs::create_dir_all(&projects).unwrap();
        let destination = projects.join(name);
        fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/claude")
                .join(name),
            &destination,
        )
        .unwrap();
        destination
    }

    #[test]
    fn hydrating_a_continuation_before_its_origin_reports_it_and_resolves_later() {
        let dir = tempfile::tempdir().unwrap();
        let branch = claude_fixture(dir.path(), "cross-file-parent.jsonl");
        let origin = claude_fixture(dir.path(), "original-session.jsonl");
        let branch_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
        let origin_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", branch_id, Some(&branch));
        catalog_row(&conn, "claude", origin_id, Some(&origin));
        drop(conn);

        // The branch is hydrated first. Its first record answers
        // `u-original-asst`, which nothing has indexed, so the hydration says
        // so rather than guessing or silently recording nothing.
        let first =
            hydrate_session_at_with_home(&db, &options("claude", branch_id), dir.path()).unwrap();
        assert!(first.diagnostics.iter().any(|diagnostic| diagnostic.code
            == "RELATIONSHIP_CONTINUITY_UNRESOLVED"
            && diagnostic.message.contains("u-original-asst")));
        let conn = open_db(&db).unwrap();
        let edges: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships WHERE relationship = 'continuation'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(edges, 0);
        drop(conn);

        // Hydrating the origin resolves it. The branch transcript is not read
        // again: its evidence was banked on the first pass.
        let branch_mtime = fs::metadata(&branch).unwrap().modified().unwrap();
        hydrate_session_at_with_home(&db, &options("claude", origin_id), dir.path()).unwrap();
        assert_eq!(
            fs::metadata(&branch).unwrap().modified().unwrap(),
            branch_mtime
        );

        let conn = open_db(&db).unwrap();
        let edge: (String, String, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT parent_session_id, relationship, child_session_id, evidence_ref \
                 FROM session_relationships WHERE relationship = 'continuation'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            edge,
            (
                origin_id.to_string(),
                "continuation".to_string(),
                Some(branch_id.to_string()),
                Some("u-original-asst".to_string()),
            )
        );
        // And the branch stops reporting itself as unresolved.
        let settled =
            hydrate_session_at_with_home(&db, &options("claude", branch_id), dir.path()).unwrap();
        assert!(!settled
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "RELATIONSHIP_CONTINUITY_UNRESOLVED"));
    }

    #[test]
    fn a_resume_marker_reaches_the_relationship_graph_through_hydration() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = claude_fixture(dir.path(), "resume-marker.jsonl");
        let resumed = "99999999-9999-9999-9999-999999999999";
        let prior = "11111111-1111-1111-1111-111111111111";
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", resumed, Some(&transcript));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("claude", resumed), dir.path()).unwrap();
        let conn = open_db(&db).unwrap();
        let relationships = crate::session_relationships(&conn, "claude", resumed).unwrap();
        // Continuity is reported on its own array; delegation reads exactly
        // what it read before continuity existed.
        assert!(relationships.as_parent.is_empty());
        assert!(relationships.as_child.is_empty());
        assert_eq!(relationships.continuity.len(), 1);
        let edge = &relationships.continuity[0];
        assert_eq!(edge.relationship, "resume");
        assert_eq!(edge.parent_session_id, prior);
        assert_eq!(edge.child_session_id.as_deref(), Some(resumed));
        assert_eq!(edge.origin_session_id.as_deref(), Some(prior));
        assert!(edge.child_has_events);
    }

    #[test]
    fn a_continued_session_is_not_a_related_session_of_its_origin() {
        // `relatedSessionIds` is what this hydration also acquired evidence
        // for. A continuation is a different conversation that carried on from
        // this one, not work it delegated — counting it would put a separate
        // session's events into this hydration's totals and hand the caller
        // sessions it never asked to acquire.
        let dir = tempfile::tempdir().unwrap();
        let projects = dir.path().join(".claude/projects/app");
        fs::create_dir_all(&projects).unwrap();
        let origin = projects.join("origin.jsonl");
        fs::write(
            &origin,
            concat!(
                "{\"sessionId\":\"origin\",\"uuid\":\"origin-u\",\"parentUuid\":null,",
                "\"type\":\"user\",\"cwd\":\"/work/app\",\"message\":{\"role\":\"user\",",
                "\"content\":\"start\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
                "{\"sessionId\":\"origin\",\"uuid\":\"origin-a\",\"parentUuid\":\"origin-u\",",
                "\"type\":\"assistant\",\"cwd\":\"/work/app\",\"message\":{\"role\":\"assistant\",",
                "\"content\":\"ok\"},\"timestamp\":\"2026-08-31T10:00:01Z\"}\n",
            ),
        )
        .unwrap();
        let follower = projects.join("follower.jsonl");
        fs::write(
            &follower,
            concat!(
                "{\"sessionId\":\"follower\",\"uuid\":\"follow-u\",\"parentUuid\":\"origin-a\",",
                "\"type\":\"user\",\"cwd\":\"/work/app\",\"message\":{\"role\":\"user\",",
                "\"content\":\"carry on\"},\"timestamp\":\"2026-08-31T11:00:00Z\"}\n",
            ),
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "origin", Some(&origin));
        catalog_row(&conn, "claude", "follower", Some(&follower));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("claude", "follower"), dir.path()).unwrap();
        let result =
            hydrate_session_at_with_home(&db, &options("claude", "origin"), dir.path()).unwrap();

        // The positive control: the continuation really was recorded, so this
        // is not an assertion over an empty relationship table.
        let conn = open_db(&db).unwrap();
        let continuation: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships \
                 WHERE relationship = 'continuation' AND parent_session_id = 'origin'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(continuation, 1);
        assert!(
            result.related_session_ids.is_empty(),
            "a continuation is not a delegated thread: {:?}",
            result.related_session_ids
        );
    }

    /// The raw-fact columns are added by migration, but their values live only
    /// in the transcript. A database indexed by an older parser must re-read
    /// the file once so the rows already stored gain them; without the parser
    /// version bump those rows would stay null forever, which reads exactly
    /// like a provider that never recorded the facts.
    #[test]
    fn a_stale_parser_version_backfills_the_per_message_raw_facts() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join(".claude/projects/app/session-facts.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            concat!(
                r#"{"sessionId":"session-facts","uuid":"u1","cwd":"/work/app","type":"user","message":{"role":"user","content":"first prompt"},"timestamp":"2026-08-31T10:00:00Z","version":"2.1.96"}"#, "\n",
                r#"{"sessionId":"session-facts","uuid":"a1","cwd":"/work/app","type":"assistant","requestId":"req_1","version":"2.1.96","message":{"role":"assistant","stop_reason":"end_turn","content":[{"type":"text","text":"done"}]},"timestamp":"2026-08-31T10:00:01Z"}"#, "\n",
            ),
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", "session-facts", Some(&transcript));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("claude", "session-facts"), dir.path())
                .unwrap();
        assert_eq!(first.status, "hydrated");
        assert_eq!(first.evidence.events, 2);

        // Exactly what an upgraded database looks like: the columns exist,
        // because the migration added them, and every row is null, because
        // nothing has re-read the transcript yet.
        let conn = open_db(&db).unwrap();
        conn.execute(
            "UPDATE session_events SET request_id=NULL, stop_reason=NULL, agent_version=NULL, \
             is_sidechain=NULL, is_meta=NULL, turn_id=NULL",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE observation_hydration_checkpoints SET parser_version = 0 \
             WHERE source='claude' AND session_id='session-facts'",
            [],
        )
        .unwrap();
        drop(conn);

        let reparsed =
            hydrate_session_at_with_home(&db, &options("claude", "session-facts"), dir.path())
                .unwrap();
        assert_eq!(reparsed.status, "updated");

        let conn = open_db(&db).unwrap();
        let row: (Option<String>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT request_id, stop_reason, agent_version FROM session_events \
                 WHERE role = 'assistant'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (
                Some("req_1".into()),
                Some("end_turn".into()),
                Some("2.1.96".into())
            )
        );
        drop(conn);

        // And the backfill happens once: the next hydration has nothing to do.
        let settled =
            hydrate_session_at_with_home(&db, &options("claude", "session-facts"), dir.path())
                .unwrap();
        assert_eq!(settled.status, "unchanged");
    }
}
