//! Targeted, provider-bounded session evidence acquisition.

use super::cursor::{load_cursor, store_cursor, CursorKey, TranscriptCursorState};
use super::*;
use crate::observations::{self, ObservationCheckpoint, ObservationKey, SessionObservation};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use std::fs::OpenOptions;
use std::time::Instant;

/// Bumped to 3 for `bytesRead`, which reports what a hydration actually read
/// rather than what the file contains.
pub const SESSION_HYDRATION_CONTRACT_VERSION: u32 = 3;
/// Bumped to 3 for incremental transcript hydration (#173). A checkpoint
/// written by an earlier parser has no byte cursor, so the first hydration
/// after upgrading re-parses that transcript once from offset 0 and writes the
/// cursor; every hydration after that resumes from it.
///
/// Version 2 was Claude subagent transcripts carrying an `agentId` being
/// indexed under that child id.
const HYDRATION_PARSER_VERSION: i64 = 3;

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

#[derive(Debug, Clone, Serialize)]
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
    pub capability: String,
    pub discovery_state: String,
    pub presence: String,
    pub indexed_through: HydrationIndexedThrough,
    pub evidence: HydrationEvidence,
    /// Bytes this hydration read from provider files: the counter a watch loop
    /// uses to tell "the tail grew" from "the whole file was re-read". About
    /// the size of the append for an incremental pass, and small but **not
    /// zero** for an `unchanged` one — deciding nothing changed means reading
    /// head records and a bounded window of each cursor's committed prefix,
    /// and a counter that omits the reads a pass could not avoid reports the
    /// work that was optimised instead of the work that was done.
    pub bytes_read: i64,
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
    /// Complete records in the file, counted while stamping.
    ///
    /// Only the providers whose readers are not yet incremental pay for this
    /// walk. Claude and Codex leave it at zero and report what their pass
    /// actually parsed, because counting the records of a 200 MB transcript on
    /// every hydration is the very cost incremental reading exists to remove.
    records: i64,
    path: Option<PathBuf>,
    /// Claude subagent sidecars, parsed once while stamping the source so the
    /// ingestion pass does not walk and re-parse the same files.
    claude_subagents: Vec<ClaudeSubagentEvidence>,
    /// Bytes read to stamp and identify the source: the Codex `session_meta`
    /// line, the resumed sidecar metadata walks. Every provider read a
    /// hydration cannot avoid belongs in `bytesRead`, or the counter measures
    /// the walk that was optimised rather than the work that was done.
    scanned_bytes: i64,
    /// A transcript read while building this stamp was rewritten under the
    /// walk, so that walk recorded no position. Reported by the hydration that
    /// contains it, whichever walk noticed.
    scanned_superseded: bool,
    /// Every locator-keyed cursor this stamp folds a file into: Claude
    /// sidecars and their metadata documents, Codex child rollouts.
    ///
    /// The stamp is built from `file_stamp`, which is mtime and size, so each
    /// of these files needs the same bounded prefix check the session's own
    /// transcript gets before the skip path trusts it. Collected while
    /// stamping so the check does not re-walk the directory to find them.
    stamped_cursors: Vec<(String, PathBuf)>,
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
    hydrate_session_at_with_home_and_connectors(db_path, options, &home_dir(), connectors)
}

#[cfg(test)]
fn hydrate_session_at_with_home(
    db_path: &Path,
    options: &HydrateSessionOptions,
    home: &Path,
) -> Result<HydrateSessionResult> {
    hydrate_session_at_with_home_and_connectors(
        db_path,
        options,
        home,
        &crate::remote::SourceConnectorSelection::default(),
    )
}

fn hydrate_session_at_with_home_and_connectors(
    db_path: &Path,
    options: &HydrateSessionOptions,
    home: &Path,
    connectors: &crate::remote::SourceConnectorSelection,
) -> Result<HydrateSessionResult> {
    validate_options(options)?;
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
    let snapshot = source_snapshot(&conn, options, &target, home)?;
    let previous = observations::checkpoint(&conn, &local_key)?.map(|checkpoint| {
        (
            checkpoint.source_stamp,
            checkpoint.parser_version,
            checkpoint.include_related,
        )
    });
    let previous_stamp = previous.as_ref().and_then(|(stamp, _, _)| stamp.clone());
    // A session whose last pass held records back is not finished with, even
    // though its bytes have not moved. The stamp short-circuit is what decides
    // whether a pass runs at all, so leaving it to fire here would mean the
    // held records were never looked at again and the message was lost rather
    // than deferred.
    let holding_records = holds_unfinished_records(&conn, options, &snapshot)?;
    let stamp_matches = !holding_records
        && previous_stamp.as_deref() == Some(snapshot.stamp.as_str())
        && previous
            .as_ref()
            .is_some_and(|(_, parser_version, _)| *parser_version == HYDRATION_PARSER_VERSION)
        && (!options.include_related
            || previous.as_ref().is_some_and(|(_, _, included)| *included))
        && target.discovery_state.as_deref() == Some("full");
    // The stamp says the size and mtime are where they were. That is not the
    // same claim as "these are the same bytes", and this shortcut returns
    // before `TranscriptReader::open`, so the prefix hash the cursor stores
    // never got a say. A rewritten file with a restored mtime was served from
    // rows built out of bytes that no longer existed.
    //
    // Paid only when the stamp was about to skip the file. A pass that is
    // going to read the transcript anyway validates the cursor inside
    // `TranscriptReader::open`, and charging it a second window here would
    // make a 1 KiB append cost 129 KiB.
    let validated = if stamp_matches {
        unchanged_cursor_check(&conn, options, &snapshot)?
    } else {
        UnchangedCursorCheck::default()
    };
    // Provider bytes this pass read: building the stamp, plus whatever
    // validating the cursors against the files cost.
    let provider_bytes = snapshot.scanned_bytes + validated.bytes_read;

    if stamp_matches && validated.valid {
        // Not an empty outcome: deciding nothing changed meant reading the
        // root's head record, every sibling's head, each sidecar's metadata,
        // and a bounded window of each cursor's committed prefix. Reporting
        // zero for a pass that opened nine files is the same well-formed zero
        // as "nothing was read", and a watch loop cannot tell them apart.
        let outcome = IngestOutcome {
            bytes_read: provider_bytes,
            superseded: snapshot.scanned_superseded,
            ..IngestOutcome::default()
        };
        return build_result(
            &conn,
            options,
            "unchanged",
            snapshot,
            &outcome,
            started.elapsed().as_millis() as i64,
        );
    }

    // One selected provider session is one destination transaction. Provider
    // JSONL readers ignore an incomplete final record, and every evidence table
    // has a provider-native uniqueness key, so interruption followed by retry is
    // safe for both new and growing sessions.
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let cursor_key = CursorKey::Session {
        source: &options.source,
        session_id: &options.session_id,
        location: "local",
    };
    // A parser generation change invalidates every position recorded by the
    // generation before it: the same bytes now mean something else. Starting
    // from an empty cursor is what makes the first sync after an upgrade a
    // single full re-parse per transcript, after which cursors take over.
    let mut cursor = if previous
        .as_ref()
        .is_some_and(|(_, parser_version, _)| *parser_version == HYDRATION_PARSER_VERSION)
    {
        load_cursor(&tx, &cursor_key)?
    } else {
        TranscriptCursorState::default()
    };
    let indexed: IngestOutcome = ingest_selected(
        &tx,
        options,
        &target,
        snapshot.path.as_deref(),
        &snapshot.claude_subagents,
        &mut cursor,
        snapshot.records,
    )?;
    let mut indexed = indexed;
    // Stamping and identifying the source is work this hydration did.
    indexed.bytes_read += provider_bytes;
    indexed.superseded |= snapshot.scanned_superseded;
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
    tx.execute(
        "INSERT INTO session_hydration_checkpoints \
         (source, session_id, location, source_stamp, parser_version, last_event_at_ms, source_bytes, records_parsed, include_related, updated_ms) \
         VALUES (?, ?, 'local', ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, location) DO UPDATE SET \
           source_stamp = excluded.source_stamp, parser_version = excluded.parser_version, \
           last_event_at_ms = excluded.last_event_at_ms, source_bytes = excluded.source_bytes, \
           records_parsed = excluded.records_parsed, include_related = excluded.include_related, \
           updated_ms = excluded.updated_ms",
        params![
            options.source,
            options.session_id,
            snapshot.stamp,
            HYDRATION_PARSER_VERSION,
            last_event_at_ms,
            snapshot.bytes,
            indexed.records,
            options.include_related,
            now_ms(),
        ],
    )?;
    // The checkpoint row has to exist before its cursor columns are written:
    // a resume position without the checkpoint it belongs to would describe
    // evidence nothing recorded.
    store_cursor(&tx, &cursor_key, &cursor)?;
    let local_observation=local_observation.unwrap_or(SessionObservation{key:local_key,raw_locator:target.locator.clone(),source_stamp:tx.query_row("SELECT source_stamp FROM session_presences WHERE source=? AND session_id=? AND location='local'",params![options.source,options.session_id],|row|row.get(0)).optional()?.flatten(),discovery_state:"shallow".into(),access_state:"available".into(),updated_ms:now_ms()});
    save_observation_progress(
        &tx,
        &local_observation,
        &snapshot.stamp,
        snapshot.bytes,
        indexed.records,
        options.include_related,
        true,
        Some(&cursor),
    )?;
    tx.commit()?;

    let status = if previous_stamp.is_some() {
        "updated"
    } else {
        "hydrated"
    };
    build_result(
        &conn,
        options,
        status,
        snapshot,
        &indexed,
        started.elapsed().as_millis() as i64,
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
        // No local file was read: this path reports what the catalog already
        // holds for a session whose full evidence is not reachable.
        bytes_read: 0,
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
        for table in ["history", "session_events", "tool_calls", "file_edits"] {
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
            // Remote evidence arrives as records over a transport; there is no
            // local file with a byte position to resume from.
            None,
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
                None,
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
    conn.execute(
        "INSERT INTO session_hydration_checkpoints \
         (source, session_id, location, source_stamp, parser_version, last_event_at_ms, source_bytes, records_parsed, include_related, updated_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, location) DO UPDATE SET \
           source_stamp=excluded.source_stamp, parser_version=excluded.parser_version, last_event_at_ms=excluded.last_event_at_ms, \
           source_bytes=excluded.source_bytes, records_parsed=excluded.records_parsed, include_related=excluded.include_related, updated_ms=excluded.updated_ms",
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
        // Remote evidence arrives as records over a transport; no local file
        // was read, so there are no bytes to report.
        bytes_read: 0,
        evidence: evidence_counts(
            conn,
            &options.source,
            &ids,
            related_session_ids.len() as u64,
        )?,
        related_session_ids,
        diagnostics: vec![HydrationDiagnostic {
            code: diagnostic_code.to_string(),
            message: diagnostic_message.to_string(),
            duration_ms: Some(started.elapsed().as_millis() as i64),
            source_bytes: Some(source_bytes),
            records_parsed: Some(records_parsed),
        }],
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
    conn: &Connection,
    options: &HydrateSessionOptions,
    target: &CatalogTarget,
    home: &Path,
) -> Result<SourceSnapshot> {
    if options.source == "relay" {
        return Err(hydration_error(
            "HYDRATION_UNSUPPORTED",
            "Relay catalog evidence has no configured full-evidence connector",
        ));
    }
    if options.source == "opencode" {
        let configured_path = std::env::var_os("OPENCODE_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".local/share/opencode/opencode.db"));
        let locator = target.locator.as_deref().ok_or_else(|| {
            hydration_error(
                "SESSION_SOURCE_UNAVAILABLE",
                "OpenCode catalog row has no store provenance; run discoverSessions() again",
            )
        })?;
        let path = PathBuf::from(locator);
        if fs::canonicalize(&path).ok() != fs::canonicalize(&configured_path).ok() {
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
            records: 0,
            path: Some(path),
            claude_subagents: Vec::new(),
            scanned_bytes: 0,
            scanned_superseded: false,
            stamped_cursors: Vec::new(),
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
    validate_provider_path(&options.source, &path, home)?;
    let mut bytes = path.metadata()?.len() as i64;
    let incremental_reader = matches!(options.source.as_str(), "claude" | "codex");
    let records = if incremental_reader {
        0
    } else {
        complete_jsonl_records(&path)?
    };
    let mut stamp = if options.source == "grok" {
        grok_session_stamp(&path)?
    } else {
        file_stamp(&path)?
    };
    let mut scanned_bytes = 0i64;
    if options.source == "codex" {
        // Identity comes from the rollout's first record, and reading it is
        // unavoidable work this hydration did.
        scanned_bytes += read_codex_session_meta_counted(&path)?.1 as i64;
    }
    let mut subagents = Vec::new();
    let mut stamped_cursors = Vec::new();
    let mut scanned_superseded = false;
    if options.source == "claude" && options.include_related {
        let (found, sidecar_bytes, sidecar_superseded) =
            claude_subagents(conn, &path, &options.session_id)?;
        subagents = found;
        scanned_bytes += sidecar_bytes as i64;
        scanned_superseded |= sidecar_superseded;
        for evidence in &subagents {
            stamp.push('|');
            stamp.push_str(&file_stamp(&evidence.path)?);
            bytes += evidence.path.metadata()?.len() as i64;
            stamped_cursors.push(("claude".to_string(), evidence.path.clone()));
            // The metadata sidecar describes the child — its type, model and
            // spawn depth, and the tool use that started it — so a sidecar
            // that arrives or changes on its own is still new evidence.
            let metadata = claude_subagent_meta_path(&evidence.path);
            if metadata.is_file() {
                stamp.push('|');
                stamp.push_str(&file_stamp(&metadata)?);
                bytes += metadata.metadata()?.len() as i64;
                stamped_cursors.push((
                    crate::ingest::CLAUDE_SUBAGENT_META_SOURCE.to_string(),
                    metadata,
                ));
            }
        }
    }
    if options.source == "codex" && options.include_related {
        // Finding the children means reading one head record from every
        // sibling rollout in the directory, whether or not it turns out to be
        // one. That is provider I/O this hydration did.
        let (children, enumeration_bytes) = codex_children_counted(&path, &options.session_id)?;
        scanned_bytes += enumeration_bytes as i64;
        for child in children {
            stamp.push('|');
            stamp.push_str(&file_stamp(&child)?);
            bytes += child.metadata()?.len() as i64;
            stamped_cursors.push(("codex".to_string(), child));
        }
    }
    Ok(SourceSnapshot {
        stamp,
        bytes,
        records,
        path: Some(path),
        claude_subagents: subagents,
        scanned_bytes,
        scanned_superseded,
        stamped_cursors,
    })
}

/// What the skip path decided about this session's cursors, and what deciding
/// it read.
#[derive(Default)]
struct UnchangedCursorCheck {
    /// Every cursor consulted still describes the file it was written from.
    valid: bool,
    /// Provider bytes the validation windows read.
    bytes_read: i64,
}

/// Whether the cursors behind this session still describe the files on disk.
///
/// Hydration's stamp shortcut and the sync walk's `transcript_unchanged` are
/// the two places a file is skipped without being read, and they now ask the
/// same question through the same helper: is the committed prefix still
/// there? Neither advances parser state to answer it, and the cost is bounded
/// — two windows per file, at most 128 KiB, paid only on the files that were
/// about to be skipped.
///
/// Only the incremental readers have cursors to check. For the rest the stamp
/// is still all there is, and claiming otherwise would turn every hydration of
/// a Cursor or Grok session into a full re-read.
fn unchanged_cursor_check(
    conn: &Connection,
    options: &HydrateSessionOptions,
    snapshot: &SourceSnapshot,
) -> Result<UnchangedCursorCheck> {
    if !matches!(options.source.as_str(), "claude" | "codex") {
        return Ok(UnchangedCursorCheck {
            valid: true,
            bytes_read: 0,
        });
    }
    let Some(path) = snapshot.path.as_deref() else {
        return Ok(UnchangedCursorCheck {
            valid: false,
            bytes_read: 0,
        });
    };
    let mut bytes_read = 0i64;
    let session = load_cursor(
        conn,
        &CursorKey::Session {
            source: &options.source,
            session_id: &options.session_id,
            location: "local",
        },
    )?;
    // No recorded position is not a clean bill of health: there is nothing to
    // validate against, so there is nothing to be confident about. One pass
    // reads the file and writes the cursor, and the shortcut is available
    // again from then on.
    let Some(file) = session.file.as_ref() else {
        return Ok(UnchangedCursorCheck {
            valid: false,
            bytes_read,
        });
    };
    let check = crate::ingest::cursor::committed_prefix_matches(file, path);
    bytes_read += check.bytes_read as i64;
    if !check.valid {
        return Ok(UnchangedCursorCheck {
            valid: false,
            bytes_read,
        });
    }
    // Every other file the stamp folds in — Claude sidecars, their metadata
    // documents, Codex child rollouts — is a transcript like any other and
    // carries the same hazard. Each has a locator-keyed cursor, and a file
    // that has never been read has none: the stamp still covers its arrival,
    // so a missing cursor does not block the skip the way the session's own
    // missing cursor does. Driving this from the stamp's own list is what
    // stops a provider being added to the stamp and forgotten here.
    for (source, file_path) in &snapshot.stamped_cursors {
        let locator = file_path.to_string_lossy().to_string();
        let cursor = load_cursor(
            conn,
            &CursorKey::Locator {
                source,
                locator: &locator,
            },
        )?;
        let Some(file) = cursor.file.as_ref() else {
            continue;
        };
        let check = crate::ingest::cursor::committed_prefix_matches(file, file_path);
        bytes_read += check.bytes_read as i64;
        if !check.valid {
            return Ok(UnchangedCursorCheck {
                valid: false,
                bytes_read,
            });
        }
    }
    Ok(UnchangedCursorCheck {
        valid: true,
        bytes_read,
    })
}

/// Whether any transcript this session is built from ended its last pass with
/// records held back.
///
/// The session's own transcript is not the only one that defers: every Claude
/// sidecar keeps its parser state in its own locator-keyed cursor, and a
/// sidecar that held an unfinished message needs a pass to run before it can
/// release those records. Consulting only the session cursor meant the
/// unchanged shortcut returned first and the child's message stayed unindexed
/// for as long as nothing else about the session changed.
fn holds_unfinished_records(
    conn: &Connection,
    options: &HydrateSessionOptions,
    snapshot: &SourceSnapshot,
) -> Result<bool> {
    let session = load_cursor(
        conn,
        &CursorKey::Session {
            source: &options.source,
            session_id: &options.session_id,
            location: "local",
        },
    )?;
    if session
        .claude
        .is_some_and(|claude| !claude.in_progress.is_empty())
    {
        return Ok(true);
    }
    for evidence in &snapshot.claude_subagents {
        let locator = evidence.path.to_string_lossy().to_string();
        let sidecar = load_cursor(
            conn,
            &CursorKey::Locator {
                source: "claude",
                locator: &locator,
            },
        )?;
        if sidecar
            .claude
            .is_some_and(|claude| !claude.in_progress.is_empty())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_provider_path(source: &str, path: &Path, home: &Path) -> Result<()> {
    let roots = match source {
        "claude" => vec![home.join(".claude/projects")],
        "codex" => vec![
            home.join(".codex/sessions"),
            home.join(".codex/archived_sessions"),
        ],
        "cursor" => vec![home.join(".cursor/projects")],
        "grok" => vec![home.join(".grok/sessions")],
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

fn complete_jsonl_records(path: &Path) -> Result<i64> {
    let mut reader = BufReader::new(fs::File::open(path)?);
    let mut records = 0;
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            break;
        }
        if !line.ends_with('\n') {
            break;
        }
        if serde_json::from_str::<Value>(line.trim_end()).is_ok() {
            records += 1;
        }
    }
    Ok(records)
}

/// What one hydration pass read and indexed, across a session's primary
/// transcript and every sidecar it reached.
#[derive(Debug, Default)]
pub(crate) struct IngestOutcome {
    /// Records handed to a per-record indexer by this pass. This is no longer
    /// "records in the file": an incremental pass over an unchanged tail reads
    /// and parses nothing, and reporting the file's total would claim work
    /// that was not done.
    pub records: i64,
    /// Bytes this pass read. The number scope 6 of #173 is about: appending
    /// 1 KiB to a 200 MB transcript reads about 1 KiB of records, plus a fixed
    /// handful of bounded validation windows — never a function of the file's
    /// size.
    pub bytes_read: i64,
    /// The validation part of [`Self::bytes_read`].
    pub validation_bytes: i64,
    /// `message.id`s whose message was still being written, across every
    /// transcript this pass touched.
    pub in_progress: Vec<String>,
    /// At least one transcript's cursor was rejected and its file re-read from
    /// zero.
    pub rotated: bool,
    /// Deferral hit its memory ceiling somewhere and indexed an unfinished
    /// message early.
    pub deferral_overflowed: bool,
    /// Records skipped for passing the per-record ceiling.
    pub oversized_records: i64,
    /// At least one transcript was rewritten while this hydration was reading
    /// it, so that transcript's cursor was not advanced.
    pub superseded: bool,
}

impl IngestOutcome {
    fn absorb(&mut self, pass: crate::ingest::cursor::IncrementalPass) {
        self.records += pass.records;
        self.bytes_read += pass.bytes_read as i64;
        self.validation_bytes += pass.validation_bytes as i64;
        self.in_progress.extend(pass.in_progress);
        self.rotated |= pass.rotated;
        self.deferral_overflowed |= pass.deferral_overflowed;
        self.superseded |= pass.superseded;
        self.oversized_records += pass.oversized_records;
    }

    /// Fold one transcript's outcome into the hydration's.
    ///
    /// Every flag here is reported to the caller, so every flag has to make
    /// the journey. `superseded` did not, and a rewritten sidecar or child
    /// therefore recorded no cursor — correctly — while the hydration that
    /// contained it said nothing about why. A parent's diagnostics are the
    /// only place that fact surfaces.
    fn absorb_outcome(&mut self, other: IngestOutcome) {
        self.records += other.records;
        self.bytes_read += other.bytes_read;
        self.validation_bytes += other.validation_bytes;
        self.in_progress.extend(other.in_progress);
        self.rotated |= other.rotated;
        self.deferral_overflowed |= other.deferral_overflowed;
        self.superseded |= other.superseded;
        self.oversized_records += other.oversized_records;
    }
}

fn ingest_selected(
    conn: &Connection,
    options: &HydrateSessionOptions,
    target: &CatalogTarget,
    path: Option<&Path>,
    claude_subagents: &[ClaudeSubagentEvidence],
    cursor: &mut TranscriptCursorState,
    records: i64,
) -> Result<IngestOutcome> {
    // A provider whose reader still re-reads the file on every change reports
    // the whole file as what it read, and the record count the snapshot walk
    // already paid for.
    let whole_file = || IngestOutcome {
        bytes_read: path.and_then(|p| p.metadata().ok()).map_or(0, |m| m.len()) as i64,
        records,
        ..Default::default()
    };
    match options.source.as_str() {
        "claude" => ingest_claude(conn, options, path.unwrap(), claude_subagents, cursor),
        "codex" => ingest_codex(conn, options, path.unwrap(), cursor),
        "cursor" => ingest_cursor(conn, options, target, path.unwrap()).map(|()| whole_file()),
        "grok" => ingest_grok(conn, options, path.unwrap()).map(|()| whole_file()),
        "opencode" => {
            sync_opencode_session(conn, path.unwrap(), &options.session_id)?;
            Ok(IngestOutcome::default())
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
    cursor: &mut TranscriptCursorState,
) -> Result<IngestOutcome> {
    // Resumed from the same cursor document the record walk uses, so a
    // hydration of a transcript that grew by a kilobyte reads a kilobyte in
    // total. Recovering identity by reading the whole file here made the
    // incremental reader behind it pointless and its `bytes_read` a report
    // about one of the two walks rather than about the hydration.
    let mut scan = cursor.claude.clone().unwrap_or_default().scan;
    let scan_pass = scan_claude_session_file_resumed(path, &mut scan)?;
    let (meta, scanned_bytes, scan_validation, scan_superseded) = (
        scan_pass.meta,
        scan_pass.bytes_read,
        scan_pass.validation_bytes,
        scan_pass.superseded,
    );
    let meta = meta.ok_or_else(|| {
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
    let mut outcome = IngestOutcome {
        bytes_read: scanned_bytes as i64,
        validation_bytes: scan_validation as i64,
        // The metadata walk is a walk like any other: if the file moved under
        // it, this hydration says so.
        superseded: scan_superseded,
        ..Default::default()
    };
    outcome.absorb(incremental::ingest_claude_transcript_incremental(
        conn, path, None, cursor,
    )?);
    // The record walk rewrites `cursor.claude`; put the metadata walk's own
    // position back on it so both survive the same store.
    if let Some(claude) = cursor.claude.as_mut() {
        claude.scan = scan;
    }
    record_claude_remote_relationship(conn, &meta)?;
    // The snapshot already walked and parsed these sidecars to stamp them, so
    // this pass indexes that evidence instead of finding it a second time.
    // Each sidecar carries its own locator-keyed cursor, so one that grows
    // does not drag the parent transcript through a re-parse.
    for evidence in subagents {
        outcome.absorb_outcome(ingest_claude_subagent(conn, &options.session_id, evidence)?);
    }
    Ok(outcome)
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
) -> Result<IngestOutcome> {
    let locator = evidence.path.to_string_lossy().to_string();
    let mut outcome = IngestOutcome::default();
    // The `agent-*.meta.json` beside the transcript is the only record of the
    // child's type, name, model and spawn depth, so it is evidence in its own
    // right and gets its own cursor: a sidecar whose metadata changed beside an
    // untouched transcript still has to reach `session_relationships`, and a
    // transcript that grew must not drag the metadata through a re-read.
    let meta_path = claude_subagent_meta_path(&evidence.path);
    if meta_path.is_file() {
        crate::ingest::cursor::stamp_whole_file(
            conn,
            crate::ingest::CLAUDE_SUBAGENT_META_SOURCE,
            &meta_path,
        )?;
    } else {
        // The sidecar that described this child is gone. `record_relationship`
        // merges with COALESCE, so re-recording evidence that no longer
        // carries a type, a name, a model or a spawn depth leaves the old ones
        // in place: absent reads as "nothing new to say" rather than as "that
        // is no longer true". Clearing first is what makes the merge able to
        // express a removal.
        clear_claude_subagent_metadata(conn, &locator)?;
        crate::ingest::cursor::forget_locator_cursor(
            conn,
            crate::ingest::CLAUDE_SUBAGENT_META_SOURCE,
            &meta_path,
        )?;
    }
    match evidence.agent_id.as_deref() {
        Some(agent_id) => {
            outcome.absorb(incremental::ingest_claude_transcript_at_locator(
                conn,
                &evidence.path,
                Some(agent_id),
            )?);
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
                },
            )?;
        }
        None => {
            outcome.absorb(incremental::ingest_claude_transcript_at_locator(
                conn,
                &evidence.path,
                None,
            )?);
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
                },
            )?;
        }
    }
    Ok(outcome)
}

/// Forget everything a metadata sidecar said about a child, because the
/// sidecar has been deleted.
///
/// Only the fields the sidecar owns. The relationship itself, the child's
/// identity and its events are evidence from the transcript and survive.
fn clear_claude_subagent_metadata(conn: &Connection, evidence_locator: &str) -> Result<()> {
    conn.execute(
        "UPDATE session_relationships          SET child_agent_type = NULL, child_agent_name = NULL, child_model = NULL,              spawn_depth = NULL, evidence_ref = NULL          WHERE source = 'claude' AND evidence_locator = ?",
        [evidence_locator],
    )?;
    Ok(())
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
/// Returns the evidence and the bytes reading it cost: the transcript's head
/// record and the whole metadata document beside it. Both are provider files,
/// so both belong in `bytesRead`.
pub(crate) fn claude_subagent_evidence(
    path: PathBuf,
    meta: &ClaudeSessionMeta,
) -> (ClaudeSubagentEvidence, u64) {
    let (first, mut bytes_read) = first_claude_record_counted(&path);
    let record_str = |key: &str| {
        first
            .as_ref()
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let sidecar = claude_subagent_meta(&path);
    bytes_read += claude_subagent_meta_path(&path)
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let meta_str = |key: &str| {
        sidecar
            .as_ref()
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let evidence = ClaudeSubagentEvidence {
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
    };
    (evidence, bytes_read)
}

/// Every subagent transcript belonging to one parent session.
///
/// `collect_matching_files` is recursive, so this reaches both the flat
/// `agent-*.jsonl` layout and `<parentSessionId>/subagents/agent-*.jsonl`, and
/// returns them sorted by path so ingestion is deterministic.
/// Returns the evidence and the bytes the enumeration read.
///
/// Every sidecar's metadata walk resumes from that sidecar's own locator
/// cursor. Scanning each one from byte zero on every hydration was the same
/// whole-file read the parent transcript no longer does, multiplied by the
/// number of children — and equally invisible, because none of it reached
/// `bytesRead`. The walk cannot be replaced with a bounded head read: a child
/// is sometimes named by an `agentId` on a record well into the file, which
/// `a_child_named_only_by_a_later_record_still_links` pins.
fn claude_subagents(
    conn: &Connection,
    transcript: &Path,
    session_id: &str,
) -> Result<(Vec<ClaudeSubagentEvidence>, u64, bool)> {
    let Some(directory) = transcript.parent() else {
        return Ok((Vec::new(), 0, false));
    };
    let mut evidence = Vec::new();
    let mut bytes_read = 0u64;
    let mut superseded = false;
    for candidate in collect_matching_files(directory, "agent-", "jsonl")? {
        if candidate == transcript {
            continue;
        }
        let locator = candidate.to_string_lossy().to_string();
        let key = CursorKey::Locator {
            source: "claude",
            locator: &locator,
        };
        let mut cursor = load_cursor(conn, &key)?;
        let mut scan = cursor.claude.clone().unwrap_or_default().scan;
        let sidecar_pass = scan_claude_session_file_resumed(&candidate, &mut scan)?;
        let (meta, read) = (sidecar_pass.meta, sidecar_pass.bytes_read);
        superseded |= sidecar_pass.superseded;
        bytes_read += read;
        cursor.claude.get_or_insert_with(Default::default).scan = scan;
        store_cursor(conn, &key, &cursor)?;
        // A subagent transcript's records carry the PARENT's sessionId, which
        // is what ties this file to the session being hydrated.
        let Some(meta) = meta else {
            continue;
        };
        if meta.session_id != session_id {
            continue;
        }
        let (found, evidence_bytes) = claude_subagent_evidence(candidate, &meta);
        bytes_read += evidence_bytes;
        evidence.push(found);
    }
    Ok((evidence, bytes_read, superseded))
}

/// The first parseable record of a transcript, from a bounded read of its
/// head rather than the whole file, and the bytes that cost.
///
/// An unreadable file is a file with no first record, not an error that takes
/// the enumeration down with it.
fn first_claude_record_counted(path: &Path) -> (Option<Value>, u64) {
    match read_leading_records(path, 1) {
        Ok((values, bytes_read)) => (values.into_iter().next(), bytes_read),
        Err(_) => (None, 0),
    }
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

fn ingest_codex(
    conn: &Connection,
    options: &HydrateSessionOptions,
    path: &Path,
    cursor: &mut TranscriptCursorState,
) -> Result<IngestOutcome> {
    let (meta, meta_bytes) = read_codex_session_meta_counted(path)?;
    let meta = meta.ok_or_else(|| {
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
    let mut indexed = IngestOutcome {
        bytes_read: meta_bytes as i64,
        ..Default::default()
    };
    let (outcome, pass) = ingest_codex_rollout_incremental(conn, path, &meta, cursor)?;
    indexed.absorb(pass);
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
        indexed.absorb_outcome(ingest_codex_children(conn, options, path)?);
    }
    Ok(indexed)
}

fn ingest_codex_children(
    conn: &Connection,
    options: &HydrateSessionOptions,
    root_path: &Path,
) -> Result<IngestOutcome> {
    // Enumeration happens twice per hydration — once to stamp the source,
    // once here — and each pass reads every candidate's head. Counting both
    // is what makes `bytesRead` the bytes this hydration read rather than the
    // bytes it would have read if it were written differently.
    let (children, enumeration_bytes) = codex_children_counted(root_path, &options.session_id)?;
    let mut indexed = IngestOutcome {
        bytes_read: enumeration_bytes as i64,
        ..Default::default()
    };
    for candidate in children {
        let (meta, meta_bytes) = read_codex_session_meta_counted(&candidate)?;
        indexed.bytes_read += meta_bytes as i64;
        let Some(meta) = meta else {
            continue;
        };
        // A child rollout is its own transcript with its own committed
        // position, keyed by the path it was read from.
        let locator = candidate.to_string_lossy().to_string();
        let key = CursorKey::Locator {
            source: "codex",
            locator: &locator,
        };
        let mut child_cursor = load_cursor(conn, &key)?;
        let (_, pass) =
            ingest_codex_rollout_incremental(conn, &candidate, &meta, &mut child_cursor)?;
        store_cursor(conn, &key, &child_cursor)?;
        indexed.absorb(pass);
        cleanup_codex_subagent_history(conn, &meta.session_id)?;
        cleanup_codex_subagent_registration(conn, &meta.session_id)?;
        if let Some(parent_session_id) = meta.parent_session_id.as_deref() {
            record_codex_delegation(conn, parent_session_id, &meta, &candidate)?;
        }
    }
    Ok(indexed)
}

/// Returns the descendants and the bytes spent reading every candidate's head
/// record to find them. Enumeration reads one record per sibling rollout, and
/// a parent with ten candidates pays for ten of them whether or not any turn
/// out to be children.
fn codex_children_counted(
    root_path: &Path,
    parent_session_id: &str,
) -> Result<(Vec<PathBuf>, u64)> {
    let Some(directory) = root_path.parent() else {
        return Ok((Vec::new(), 0));
    };
    let mut bytes_read = 0u64;
    let mut children_by_parent: HashMap<String, Vec<(String, PathBuf)>> = HashMap::new();
    for candidate in collect_matching_files(directory, "rollout-", "jsonl")? {
        if candidate == root_path {
            continue;
        }
        let (meta, read) = read_codex_session_meta_counted(&candidate)?;
        bytes_read += read;
        let Some(meta) = meta else {
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
    Ok((descendants, bytes_read))
}

fn ingest_cursor(
    conn: &Connection,
    options: &HydrateSessionOptions,
    _target: &CatalogTarget,
    path: &Path,
) -> Result<()> {
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
    let ts_ms = file_modified_ms(path).unwrap_or(0);
    let reader = BufReader::new(fs::File::open(path)?);
    for line in reader.lines().map_while(std::result::Result::ok) {
        if let Some(project) = project.as_deref() {
            ingest_cursor_line(conn, &line, &options.session_id, project, ts_ms)?;
        }
    }
    upsert_session(
        conn,
        &options.session_id,
        "cursor",
        project.as_deref(),
        None,
        ts_ms,
        ts_ms,
        None,
        Some(&path.to_string_lossy()),
    )
}

fn ingest_grok(conn: &Connection, options: &HydrateSessionOptions, path: &Path) -> Result<()> {
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
    ingest_grok_session(conn, &session, &path.to_string_lossy())?;
    Ok(())
}

fn build_result(
    conn: &Connection,
    options: &HydrateSessionOptions,
    status: &str,
    snapshot: SourceSnapshot,
    indexed: &IngestOutcome,
    duration_ms: i64,
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
        message: format!(
            "targeted provider evidence acquisition completed; {} byte(s) read",
            indexed.bytes_read
        ),
        duration_ms: Some(duration_ms),
        source_bytes: Some(snapshot.bytes),
        records_parsed: Some(indexed.records),
    }];
    if indexed.rotated {
        diagnostics.push(HydrationDiagnostic {
            code: "HYDRATION_SOURCE_ROTATED".to_string(),
            message: "a provider transcript no longer matches the cursor recorded for it \
                      (replaced, truncated, or rewritten in place); it was re-read in full"
                .to_string(),
            duration_ms: None,
            source_bytes: Some(snapshot.bytes),
            records_parsed: Some(indexed.records),
        });
    }
    if indexed.superseded {
        diagnostics.push(HydrationDiagnostic {
            code: "HYDRATION_SOURCE_REWRITTEN".to_string(),
            message: "a provider transcript was rewritten while this pass was reading it; \
                      no cursor was recorded for it and the next pass reads it again"
                .to_string(),
            duration_ms: None,
            source_bytes: Some(snapshot.bytes),
            records_parsed: Some(indexed.records),
        });
    }
    if !indexed.in_progress.is_empty() {
        diagnostics.push(HydrationDiagnostic {
            code: "HYDRATION_IN_PROGRESS_MESSAGES".to_string(),
            message: format!(
                "{} message(s) were still being written and were not indexed: {}",
                indexed.in_progress.len(),
                indexed.in_progress.join(", ")
            ),
            duration_ms: None,
            source_bytes: None,
            records_parsed: Some(indexed.in_progress.len() as i64),
        });
    }
    if indexed.oversized_records > 0 {
        diagnostics.push(HydrationDiagnostic {
            code: "HYDRATION_OVERSIZED_RECORDS".to_string(),
            message: format!(
                "{} record(s) passed the {} byte per-record ceiling and were skipped; \
                 a record that large is evidence of corruption rather than of an \
                 unusually long turn",
                indexed.oversized_records,
                crate::ingest::cursor::MAX_RECORD_BYTES
            ),
            duration_ms: None,
            source_bytes: None,
            records_parsed: Some(indexed.oversized_records),
        });
    }
    if indexed.deferral_overflowed {
        diagnostics.push(HydrationDiagnostic {
            code: "HYDRATION_IN_PROGRESS_OVERFLOW".to_string(),
            message: "a transcript held more unfinished messages than the deferral ceiling \
                      allows; the oldest were indexed as they stood so the reader could \
                      keep making progress"
                .to_string(),
            duration_ms: None,
            source_bytes: None,
            records_parsed: None,
        });
    }
    if options.include_related {
        diagnostics.extend(unlinked_diagnostics(
            conn,
            &options.source,
            &options.session_id,
        )?);
    }
    Ok(HydrateSessionResult {
        contract_version: SESSION_HYDRATION_CONTRACT_VERSION,
        source: options.source.clone(),
        session_id: options.session_id.clone(),
        status: status.to_string(),
        capability: "full".to_string(),
        discovery_state: "full".to_string(),
        presence: "local".to_string(),
        indexed_through: HydrationIndexedThrough {
            source_stamp: Some(snapshot.stamp),
            last_event_at_ms,
        },
        evidence,
        bytes_read: indexed.bytes_read,
        related_session_ids,
        diagnostics,
    })
}

fn related_ids(conn: &Connection, source: &str, session_id: &str) -> Result<Vec<String>> {
    Ok(conn
        .prepare(
            "WITH RECURSIVE descendants(child_session_id) AS ( \
               SELECT child_session_id FROM session_relationships \
               WHERE source = ?1 AND parent_session_id = ?2 \
                 AND child_session_id IS NOT NULL AND child_session_id != ?2 \
               UNION \
               SELECT relationship.child_session_id FROM session_relationships relationship \
               JOIN descendants ON relationship.parent_session_id = descendants.child_session_id \
               WHERE relationship.source = ?1 AND relationship.child_session_id IS NOT NULL \
                 AND relationship.child_session_id != ?2 \
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

pub(crate) fn hydrate_with_provider(
    db_path: &Path,
    options: &HydrateSessionOptions,
    provider: &dyn ShallowSessionProvider,
) -> Result<HydrateSessionResult> {
    validate_options(options)?;
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
    let evidence = match provider.acquire(&home_dir(), &observation) {
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
            hydrate_session_at_with_home_and_connectors(
                db_path,
                options,
                &home_dir(),
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
    cursor: Option<&TranscriptCursorState>,
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
            committed_offset: cursor.map_or(0, |cursor| cursor.committed_offset()),
            prefix_hash: cursor.and_then(|cursor| cursor.prefix_hash()),
            dev_ino: cursor.and_then(|cursor| cursor.dev_ino()),
            parser_state_json: cursor.map(|cursor| cursor.encode()),
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
    use crate::source_evidence::{self, EvidenceKind, EvidenceRecord, FULL_SESSION_KINDS};
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
            let records =
                source_evidence::read_session(&conn, source, session_id, FULL_SESSION_KINDS)?;
            (
                source_stamp,
                source_bytes,
                FULL_SESSION_KINDS.to_vec(),
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

    fn options(source: &str, session_id: &str) -> HydrateSessionOptions {
        HydrateSessionOptions {
            source: source.to_string(),
            session_id: session_id.to_string(),
            scope: SessionScope::Local,
            include_related: true,
        }
    }

    // ---------------------------------------------------------------------
    // Incremental transcript hydration (#173).
    //
    // The three `claude/` fixtures are copied byte for byte from burn's
    // `tests/fixtures/claude/`, so the two readers are held to one corpus and
    // a disagreement between them shows up as a diff in this repository rather
    // than as two plausible answers in production.
    // ---------------------------------------------------------------------

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/claude")
            .join(name)
    }

    /// A transcript placed where the Claude provider root guard expects it,
    /// with a catalog row pointing at it.
    fn seed_claude_transcript(home: &Path, session_id: &str, bytes: &[u8]) -> PathBuf {
        let transcript = home
            .join(".claude/projects/app")
            .join(format!("{session_id}.jsonl"));
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(&transcript, bytes).unwrap();
        transcript
    }

    fn session_event_snapshot(
        db: &Path,
        session_id: &str,
    ) -> Vec<(String, String, String, String)> {
        let conn = open_db(db).unwrap();
        let mut statement = conn
            .prepare(
                "SELECT event_uid, role, kind, COALESCE(text, '') FROM session_events \
                 WHERE source = 'claude' AND session_id = ? ORDER BY event_uid",
            )
            .unwrap();
        let rows = statement
            .query_map([session_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        rows
    }

    fn diagnostic<'a>(
        result: &'a HydrateSessionResult,
        code: &str,
    ) -> Option<&'a HydrationDiagnostic> {
        result
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == code)
    }

    /// Put a cursor's quiet-since stamp far enough back that the next pass
    /// treats the file as abandoned.
    ///
    /// Reaching into the stored stamp rather than sleeping keeps these tests
    /// deterministic and instant: the rule under test is "the file has been
    /// still for longer than the grace window", and the window's length is
    /// not what any of them is about.
    fn age_past_grace(db: &Path, key: &CursorKey<'_>) {
        let conn = open_db(db).unwrap();
        let mut cursor = load_cursor(&conn, key).unwrap();
        let file = cursor
            .file
            .as_mut()
            .expect("a cursor holding records has a file position");
        file.unchanged_since_ms = now_ms() - super::cursor::QUIESCENT_GRACE_MS - 1;
        store_cursor(&conn, key, &cursor).unwrap();
    }

    /// Move a file's mtime forward by a second.
    ///
    /// A rewrite between two calls in the same test can land inside one
    /// filesystem timestamp tick, which would make the fixture prove the
    /// opposite of what it claims: that a rewrite with an *unchanged* stat is
    /// caught. The tests that need a moved stat say so explicitly.
    fn bump_mtime(path: &Path) {
        let moved = fs::metadata(path).unwrap().modified().unwrap() + Duration::from_secs(1);
        fs::File::open(path).unwrap().set_modified(moved).unwrap();
    }

    /// Hydrate, and report what of `bytes_read` was records rather than the
    /// bounded windows the pass hashed to validate its cursors.
    ///
    /// The validation figure comes from the code that spends it, not from the
    /// rule restated in the test — a test that recomputes the thing it is
    /// checking passes when both are wrong together.
    fn hydrate_counting_records(
        db: &Path,
        options: &HydrateSessionOptions,
        home: &Path,
    ) -> (HydrateSessionResult, i64) {
        super::cursor::reset_validation_meter();
        let result = hydrate_session_at_with_home(db, options, home).unwrap();
        let records = result.bytes_read - super::cursor::validation_meter() as i64;
        (result, records)
    }

    fn session_cursor_key<'a>(session_id: &'a str) -> CursorKey<'a> {
        CursorKey::Session {
            source: "claude",
            session_id,
            location: "local",
        }
    }

    fn stored_cursor(db: &Path, session_id: &str) -> TranscriptCursorState {
        let conn = open_db(db).unwrap();
        load_cursor(
            &conn,
            &CursorKey::Session {
                source: "claude",
                session_id,
                location: "local",
            },
        )
        .unwrap()
    }

    /// The tail of `incomplete-then-complete.jsonl` with `stop_reason` set,
    /// exactly as burn's incremental test completes that message.
    fn incomplete_fixture_completion() -> String {
        serde_json::json!({
            "parentUuid": "u-asst-2",
            "isSidechain": false,
            "message": {
                "model": "claude-sonnet-4-6",
                "id": "msg_inprog_1",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": "finished"}],
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": 7,
                    "output_tokens": 3,
                    "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0
                }
            },
            "type": "assistant",
            "uuid": "u-asst-3",
            "timestamp": "2026-04-20T00:00:03.000Z",
            "cwd": "/tmp/project",
            "sessionId": "33333333-3333-3333-3333-333333333333"
        })
        .to_string()
            + "\n"
    }

    const INCOMPLETE_SESSION: &str = "33333333-3333-3333-3333-333333333333";
    /// Byte offset of the `msg_inprog_1` line in the shipped fixture.
    const INCOMPLETE_INPROGRESS_OFFSET: i64 = 838;

    #[test]
    fn an_unfinished_message_is_held_back_and_then_indexed_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = fs::read(fixture("incomplete-then-complete.jsonl")).unwrap();
        let transcript = seed_claude_transcript(dir.path(), INCOMPLETE_SESSION, &bytes);
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", INCOMPLETE_SESSION, Some(&transcript));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("claude", INCOMPLETE_SESSION), dir.path())
                .unwrap();
        assert_eq!(first.status, "hydrated");

        // The finished message is indexed; the one still streaming is not.
        let events = session_event_snapshot(&db, INCOMPLETE_SESSION);
        let uids: Vec<&str> = events.iter().map(|row| row.0.as_str()).collect();
        assert!(uids.contains(&"u-asst-1:0"), "{uids:?}");
        assert!(
            !uids.iter().any(|uid| uid.starts_with("u-asst-2")),
            "an unfinished assistant message must not be indexed: {uids:?}"
        );

        let held = diagnostic(&first, "HYDRATION_IN_PROGRESS_MESSAGES")
            .expect("a held message is reported");
        assert_eq!(held.records_parsed, Some(1));
        assert!(held.message.contains("msg_inprog_1"), "{}", held.message);

        // The cursor backs up to the first byte of the held message, so the
        // next pass re-reads it rather than resuming past it.
        let cursor = stored_cursor(&db, INCOMPLETE_SESSION);
        assert_eq!(cursor.committed_offset(), INCOMPLETE_INPROGRESS_OFFSET);
        assert_eq!(
            cursor.claude.as_ref().unwrap().in_progress,
            vec!["msg_inprog_1".to_string()]
        );

        // The completion arrives.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        write!(file, "{}", incomplete_fixture_completion()).unwrap();
        drop(file);

        let (second, second_records) =
            hydrate_counting_records(&db, &options("claude", INCOMPLETE_SESSION), dir.path());
        assert_eq!(second.status, "updated");
        assert!(diagnostic(&second, "HYDRATION_IN_PROGRESS_MESSAGES").is_none());
        assert!(diagnostic(&second, "HYDRATION_SOURCE_ROTATED").is_none());

        // Both records of the completed message are present, once each, and
        // the usage carrier is stored with them.
        let events = session_event_snapshot(&db, INCOMPLETE_SESSION);
        let uids: Vec<&str> = events.iter().map(|row| row.0.as_str()).collect();
        assert_eq!(
            uids,
            vec!["u-asst-1:0", "u-asst-2:0", "u-asst-3:0", "u-user-1:0"],
            "every record is indexed exactly once"
        );
        let conn = open_db(&db).unwrap();
        let usage: String = conn
            .query_row(
                "SELECT COALESCE(token_json, '') FROM session_events \
                 WHERE source = 'claude' AND session_id = ? AND event_uid = 'u-asst-3:0'",
                [INCOMPLETE_SESSION],
                |row| row.get(0),
            )
            .unwrap();
        assert!(usage.contains("\"output_tokens\":3"), "{usage}");

        // Only the appended bytes were read on the second pass, plus the held
        // message the record walk declined to commit past. The metadata walk
        // holds nothing back, so it reads the append alone.
        let appended = incomplete_fixture_completion().len() as i64;
        let held_line = bytes.len() as i64 - INCOMPLETE_INPROGRESS_OFFSET;
        // Records only: a pass also hashes bounded windows to validate its
        // cursor, and those bytes are in `bytes_read` too. Reported by the
        // code that spends them, so this stays an exact statement about the
        // records.
        assert_eq!(second_records, 2 * appended + held_line);
        assert_eq!(
            stored_cursor(&db, INCOMPLETE_SESSION).committed_offset(),
            bytes.len() as i64 + appended
        );
    }

    /// One pass over the whole file and three passes over growing prefixes of
    /// it must leave the same rows. Run against both interleaving fixtures.
    fn assert_append_chunks_match_one_pass(name: &str, session_id: &str) {
        let bytes = fs::read(fixture(name)).unwrap();
        let line_starts: Vec<usize> = std::iter::once(0)
            .chain(
                bytes
                    .iter()
                    .enumerate()
                    .filter(|(_, byte)| **byte == b'\n')
                    .map(|(index, _)| index + 1),
            )
            .filter(|start| *start < bytes.len())
            .collect();
        assert!(
            line_starts.len() >= 3,
            "{name} needs at least three records to append in three chunks"
        );
        // Three chunks: first record, up to the second-to-last, then the rest.
        let cuts = [
            line_starts[1],
            line_starts[line_starts.len() - 1],
            bytes.len(),
        ];

        let whole_dir = tempfile::tempdir().unwrap();
        let whole_db = whole_dir.path().join("history.db");
        let whole_transcript = seed_claude_transcript(whole_dir.path(), session_id, &bytes);
        let conn = open_db(&whole_db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&whole_transcript));
        drop(conn);
        hydrate_session_at_with_home(&whole_db, &options("claude", session_id), whole_dir.path())
            .unwrap();
        let expected = session_event_snapshot(&whole_db, session_id);
        assert!(!expected.is_empty(), "{name} produced no events");

        let grown_dir = tempfile::tempdir().unwrap();
        let grown_db = grown_dir.path().join("history.db");
        let grown_transcript =
            seed_claude_transcript(grown_dir.path(), session_id, &bytes[..cuts[0]]);
        let conn = open_db(&grown_db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&grown_transcript));
        drop(conn);
        hydrate_session_at_with_home(&grown_db, &options("claude", session_id), grown_dir.path())
            .unwrap();
        for window in cuts.windows(2) {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&grown_transcript)
                .unwrap();
            file.write_all(&bytes[window[0]..window[1]]).unwrap();
            drop(file);
            let result = hydrate_session_at_with_home(
                &grown_db,
                &options("claude", session_id),
                grown_dir.path(),
            )
            .unwrap();
            assert!(
                diagnostic(&result, "HYDRATION_SOURCE_ROTATED").is_none(),
                "{name} must not rotate on a plain append"
            );
        }

        assert_eq!(
            session_event_snapshot(&grown_db, session_id),
            expected,
            "{name} must hydrate identically in one pass and in three appends"
        );
    }

    #[test]
    fn interleaved_turns_hydrate_identically_whole_or_in_three_appends() {
        assert_append_chunks_match_one_pass(
            "interleaved-turns.jsonl",
            "44444444-4444-4444-4444-444444444444",
        );
    }

    #[test]
    fn an_out_of_order_parent_chain_hydrates_identically_whole_or_in_three_appends() {
        assert_append_chunks_match_one_pass(
            "parent-chain-out-of-order.jsonl",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        );
    }

    #[test]
    fn a_transcript_truncated_below_its_cursor_is_re_read_from_zero() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = fs::read(fixture("interleaved-turns.jsonl")).unwrap();
        let session_id = "44444444-4444-4444-4444-444444444444";
        let transcript = seed_claude_transcript(dir.path(), session_id, &bytes);
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert!(diagnostic(&first, "HYDRATION_SOURCE_ROTATED").is_none());
        let committed = stored_cursor(&db, session_id).committed_offset();
        assert_eq!(committed, bytes.len() as i64);
        let expected = session_event_snapshot(&db, session_id);

        // The provider replaces the session with a shorter one, cut at a
        // record boundary so the pass has no partial tail to withhold and the
        // byte count below is exact. The cursor is past the new end of file,
        // which is not a position in this file at all.
        let first_line_end = bytes.iter().position(|byte| *byte == b'\n').unwrap() + 1;
        let truncated = &bytes[..first_line_end];
        fs::write(&transcript, truncated).unwrap();
        let (rotated, rotated_records) =
            hydrate_counting_records(&db, &options("claude", session_id), dir.path());
        let diagnostic = diagnostic(&rotated, "HYDRATION_SOURCE_ROTATED")
            .expect("truncation below the cursor is reported");
        assert!(diagnostic.message.contains("re-read in full"));
        // Re-read from zero: every byte of the shorter file, by both walks,
        // not a resume.
        // Records only: a pass also hashes bounded windows to validate its
        // cursor, and those bytes are in `bytes_read` too. Reported by the
        // code that spends them, so this stays an exact statement about the
        // records.
        assert_eq!(rotated_records, 2 * truncated.len() as i64);
        assert_eq!(
            stored_cursor(&db, session_id).committed_offset(),
            truncated.len() as i64
        );
        // The rows the longer file produced are still there; nothing on this
        // path deletes evidence, it only stops resuming past it.
        assert!(session_event_snapshot(&db, session_id).len() <= expected.len());
    }

    #[test]
    fn a_rewritten_prefix_is_caught_even_at_the_same_length() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = fs::read(fixture("interleaved-turns.jsonl")).unwrap();
        let session_id = "44444444-4444-4444-4444-444444444444";
        let transcript = seed_claude_transcript(dir.path(), session_id, &bytes);
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);
        hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();

        // Same length, same mtime ordering, different bytes at the head: the
        // size and mtime terms of the rotation rule cannot see this, and the
        // prefix window is what catches it.
        let mut rewritten = bytes.clone();
        rewritten[10] = if rewritten[10] == b'x' { b'y' } else { b'x' };
        fs::write(&transcript, &rewritten).unwrap();
        let result =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert!(
            diagnostic(&result, "HYDRATION_SOURCE_ROTATED").is_some(),
            "a rewritten prefix at an unchanged length must rotate the cursor"
        );
    }

    #[test]
    fn upgrading_the_parser_generation_re_reads_once_and_then_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = fs::read(fixture("interleaved-turns.jsonl")).unwrap();
        let session_id = "44444444-4444-4444-4444-444444444444";
        let transcript = seed_claude_transcript(dir.path(), session_id, &bytes);
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);
        hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();

        // Stand in for a database written by the parser generation before
        // this one: the checkpoint is at an older version and has no cursor,
        // which is exactly what every existing install looks like on the first
        // sync after upgrading.
        let conn = open_db(&db).unwrap();
        conn.execute(
            "UPDATE session_hydration_checkpoints \
             SET parser_version = ?, committed_offset = 0, prefix_hash = NULL, \
                 dev_ino = NULL, parser_state_json = NULL \
             WHERE source = 'claude' AND session_id = ?",
            params![HYDRATION_PARSER_VERSION - 1, session_id],
        )
        .unwrap();
        conn.execute(
            "UPDATE observation_hydration_checkpoints SET parser_version = ? \
             WHERE source = 'claude' AND session_id = ?",
            params![HYDRATION_PARSER_VERSION - 1, session_id],
        )
        .unwrap();
        drop(conn);

        // One full re-parse from offset 0 - the whole file, and not reported
        // as a rotation, because nothing about the file changed.
        let (upgraded, upgraded_records) =
            hydrate_counting_records(&db, &options("claude", session_id), dir.path());
        assert_eq!(upgraded.status, "updated");
        // Records only: a pass also hashes bounded windows to validate its
        // cursor, and those bytes are in `bytes_read` too. Reported by the
        // code that spends them, so this stays an exact statement about the
        // records.
        assert_eq!(upgraded_records, 2 * bytes.len() as i64);
        assert!(diagnostic(&upgraded, "HYDRATION_SOURCE_ROTATED").is_none());

        // And from here the cursor carries: an append reads only the append.
        let addition = incomplete_fixture_completion();
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        write!(file, "{addition}").unwrap();
        drop(file);
        let (_resumed, resumed_records) =
            hydrate_counting_records(&db, &options("claude", session_id), dir.path());
        // Records only; the bounded validation windows are counted too and
        // reported by the code that spends them.
        assert_eq!(resumed_records, 2 * addition.len() as i64);
    }

    #[test]
    fn a_sidecar_that_grows_does_not_re_read_its_parent() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-sidecar";
        let transcript = seed_claude_transcript(
            dir.path(),
            session_id,
            b"{\"sessionId\":\"session-sidecar\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"parent prompt\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
        );
        let sidecar = transcript.parent().unwrap().join("agent-child.jsonl");
        let first_record = "{\"sessionId\":\"session-sidecar\",\"agentId\":\"child-1\",\"isSidechain\":true,\"uuid\":\"s1\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"first\"},\"timestamp\":\"2026-08-31T10:00:01Z\"}\n";
        fs::write(&sidecar, first_record).unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);
        hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();

        let addition = "{\"sessionId\":\"session-sidecar\",\"agentId\":\"child-1\",\"isSidechain\":true,\"uuid\":\"s2\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"second\"},\"timestamp\":\"2026-08-31T10:00:02Z\"}\n";
        let mut file = fs::OpenOptions::new().append(true).open(&sidecar).unwrap();
        write!(file, "{addition}").unwrap();
        drop(file);

        let (_grown, grown_records) =
            hydrate_counting_records(&db, &options("claude", session_id), dir.path());
        // The sidecar's new line, read by its metadata walk and its record
        // walk, plus its *first* record, which building the child's evidence
        // reads from the head however far the file has grown. The parent
        // transcript, which did not change, contributes nothing.
        // Records only: a pass also hashes bounded windows to validate its
        // cursor, and those bytes are in `bytes_read` too. Reported by the
        // code that spends them, so this stays an exact statement about the
        // records.
        assert_eq!(
            grown_records,
            2 * addition.len() as i64 + first_record.len() as i64
        );
        assert_eq!(
            session_event_snapshot(&db, "child-1")
                .iter()
                .map(|row| row.0.clone())
                .collect::<Vec<_>>(),
            vec!["s1:0".to_string(), "s2:0".to_string()]
        );
    }

    /// Peak resident set size of this process, in bytes.
    ///
    /// `getrusage(RUSAGE_SELF).ru_maxrss` is the high-water mark since the
    /// process started, which is what a memory *ceiling* needs: a sample taken
    /// after the fact would miss a transient spike, and a transient spike is
    /// exactly how "read the whole file into a String" fails. The unit differs
    /// by platform and the man pages disagree with each other, so it is
    /// normalized here: Linux reports kilobytes, macOS and the other BSDs
    /// report bytes.
    #[cfg(unix)]
    fn peak_rss_bytes() -> u64 {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        // SAFETY: `usage` is a valid, fully initialized `rusage`.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
            return 0;
        }
        let raw = usage.ru_maxrss.max(0) as u64;
        if cfg!(target_os = "macos") {
            raw
        } else {
            raw * 1024
        }
    }

    /// Write a synthetic Claude transcript of at least `target_bytes`.
    ///
    /// Generated into the test's own temporary directory and deleted with it.
    /// A 200 MB fixture is not committed: the repository would carry it
    /// forever to prove a property that is about the reader, not the bytes.
    fn write_large_claude_transcript(path: &Path, session_id: &str, target_bytes: u64) -> u64 {
        use std::io::BufWriter;
        let file = fs::File::create(path).unwrap();
        let mut writer = BufWriter::with_capacity(1 << 20, file);
        // Roughly 2 KiB of assistant text per record, so the reader meets
        // records large enough that holding even a few thousand would show.
        let filler = "x".repeat(2048);
        let mut written = 0u64;
        let mut index = 0u64;
        while written < target_bytes {
            let record = format!(
                "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u{index}\",\"cwd\":\"/work/app\",\
                 \"type\":\"assistant\",\"message\":{{\"id\":\"msg_{index}\",\"role\":\"assistant\",\
                 \"stop_reason\":\"end_turn\",\"content\":[{{\"type\":\"text\",\"text\":\"{filler}\"}}]}},\
                 \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
            );
            writer.write_all(record.as_bytes()).unwrap();
            written += record.len() as u64;
            index += 1;
        }
        writer.flush().unwrap();
        drop(writer);
        written
    }

    /// Scope 6 of #173: reading and indexing a transcript far larger than
    /// memory stays under a documented ceiling, and a small append to it
    /// afterwards reads only the append.
    ///
    /// **What is measured, and what is not.** This drives the incremental
    /// reader over a 200 MB transcript against a real on-disk database in
    /// autocommit, which is the reader plus the per-record writes it feeds.
    /// It deliberately does *not* go through [`hydrate_session_at`], because
    /// that wraps the whole session in one `Immediate` transaction and a
    /// transaction's own cost is not the reader's. Measured on this machine,
    /// the split is unambiguous:
    ///
    /// | path                          | peak RSS growth over 100 MB |
    /// |-------------------------------|-----------------------------|
    /// | reader alone, no rows written | 0 bytes                     |
    /// | reader + one open transaction | ~52 MB (≈ 0.5 × file)       |
    ///
    /// So the reader is O(1) in the file and the single transaction is
    /// linear in it. Chunked commits — scope 2's "commit every
    /// `JSONL_CHUNK_LINES`" — are what bound the second term, and they are
    /// deferred: hydration's one-transaction-per-session boundary is relied
    /// on for rollback by several existing tests and splitting it is a
    /// separate change. Until then this asserts the term this change owns.
    ///
    /// **How the bound is measured.** `getrusage(RUSAGE_SELF).ru_maxrss` is
    /// the process's high-water mark since it started, which is what a
    /// ceiling needs: a sample taken afterwards would miss a transient spike,
    /// and a transient spike is exactly how "read the whole file into a
    /// String" fails. Linux reports it in kilobytes and macOS in bytes, which
    /// [`peak_rss_bytes`] normalizes. The assertion is on the *growth* of the
    /// peak, since the harness allocated whatever it allocated before this
    /// test ran. 64 MiB over a 200 MB file is generous enough not to be flaky
    /// on a loaded runner and three times under what a whole-file read
    /// produces, so a regression to `read_to_string` fails it outright rather
    /// than squeaking under.
    ///
    /// **Why `#[ignore]`.** Not because a CI runner cannot host 200 MB, but
    /// because `ru_maxrss` is a per-*process* high-water mark and `cargo test`
    /// runs this suite as threads of one process. Another test's allocation
    /// inflates the reading, and the resulting failure would be real-looking,
    /// unreproducible, and nothing to do with this reader. Run it alone:
    ///
    /// ```text
    /// cargo test -p ai-hist --all-features --lib -- --ignored --exact \
    ///   --test-threads=1 \
    ///   ingest::hydrate::tests::reading_a_200mb_transcript_stays_under_a_memory_ceiling
    /// ```
    ///
    /// The `bytes_read` half of the scope runs on every CI pass, at a size
    /// that costs nothing, in
    /// `an_append_to_a_large_transcript_reads_only_the_append`.
    #[test]
    #[ignore = "measures process-wide peak RSS; run alone with --test-threads=1"]
    fn reading_a_200mb_transcript_stays_under_a_memory_ceiling() {
        const TARGET_BYTES: u64 = 200 * 1024 * 1024;
        const CEILING_BYTES: u64 = 64 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-large";
        let transcript = dir.path().join("large.jsonl");
        let size = write_large_claude_transcript(&transcript, session_id, TARGET_BYTES);
        assert!(size >= TARGET_BYTES);

        let conn = open_db(&dir.path().join("history.db")).unwrap();
        // Durability is not what this measures, and 200k autocommit fsyncs
        // would make it a stopwatch test instead of a memory one.
        conn.pragma_update(None, "synchronous", "OFF").unwrap();

        let mut cursor = TranscriptCursorState::default();
        let before = peak_rss_bytes();
        let pass = incremental::ingest_claude_transcript_incremental(
            &conn,
            &transcript,
            None,
            &mut cursor,
        )
        .unwrap();
        let after = peak_rss_bytes();

        assert_eq!(pass.bytes_read, size);
        assert!(pass.records > 0);
        let indexed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = ?",
                [session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            indexed, pass.records,
            "every record read must have produced a row"
        );

        let growth = after.saturating_sub(before);
        assert!(
            growth < CEILING_BYTES,
            "reading and indexing {size} bytes grew peak RSS by {growth} bytes, \
             over the {CEILING_BYTES} byte ceiling"
        );

        // And the point of all of it: one more kilobyte costs one more
        // kilobyte of reading, not another pass over the file.
        let addition = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"tail\",\"cwd\":\"/work/app\",\
             \"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"{}\"}},\
             \"timestamp\":\"2026-08-31T11:00:00Z\"}}\n",
            "y".repeat(900)
        );
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        write!(file, "{addition}").unwrap();
        drop(file);
        let second = incremental::ingest_claude_transcript_incremental(
            &conn,
            &transcript,
            None,
            &mut cursor,
        )
        .unwrap();
        assert_eq!(second.bytes_read, addition.len() as u64);
    }

    /// The same property at a size CI can afford on every run: after a first
    /// pass over a transcript of many records, appending one record reads only
    /// that record's bytes.
    ///
    /// `bytes_read` is the **whole hydration's** total, which is the only
    /// figure worth asserting. It once covered the record walk alone while a
    /// metadata walk ahead of it read the file from the start, so a 1 KiB
    /// append to a 200 MB transcript read 200 MB and reported about 1 KiB —
    /// the shape of a success, computed over one of the two walks.
    #[test]
    fn an_append_to_a_large_transcript_reads_only_the_append() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-append";
        let transcript = dir
            .path()
            .join(".claude/projects/app")
            .join(format!("{session_id}.jsonl"));
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        // Larger than the 64 KiB prefix window from both ends, so the cursor
        // validation this exercises is the real two-window case rather than
        // the degenerate "the window is the whole file" one.
        let size = write_large_claude_transcript(&transcript, session_id, 4 * 1024 * 1024);

        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let (_first, first_records) =
            hydrate_counting_records(&db, &options("claude", session_id), dir.path());
        // Both walks read the whole file once.
        // Records only: a pass also hashes bounded windows to validate its
        // cursor, and those bytes are in `bytes_read` too. Reported by the
        // code that spends them, so this stays an exact statement about the
        // records.
        assert_eq!(first_records, 2 * size as i64);
        let events_after_first = session_event_snapshot(&db, session_id).len();

        let addition = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"tail\",\"cwd\":\"/work/app\",\
             \"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"{}\"}},\
             \"timestamp\":\"2026-08-31T11:00:00Z\"}}\n",
            "y".repeat(900)
        );
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        write!(file, "{addition}").unwrap();
        drop(file);

        let (appended, appended_records) =
            hydrate_counting_records(&db, &options("claude", session_id), dir.path());
        // Records only; the bounded validation windows are counted too and
        // reported by the code that spends them.
        assert_eq!(
            appended_records,
            2 * addition.len() as i64,
            "an append must cost its own size across both walks, not the file's"
        );
        // And the whole pass, validation included, is still nowhere near the
        // file: bounded windows, not a re-read.
        assert!(appended.bytes_read * 4 < size as i64);
        assert_eq!(
            session_event_snapshot(&db, session_id).len(),
            events_after_first + 1
        );
    }

    /// The last record of a transcript that does not end in a newline is still
    /// that transcript's last record.
    ///
    /// Regression for the `optional-history-plugins` CI failure on #204: the
    /// plugin SDK drives a real sync over a 525-record fixture built with
    /// `records.join('\n')` — no trailing newline — and got 524. Withholding
    /// every unterminated line is right for a half-written one and wrong for a
    /// provider that simply does not terminate its last, and nothing in the
    /// bytes tells the two apart. Parsing does: a half-written line is not
    /// complete JSON.
    ///
    /// The count is the point. An off-by-one here is invisible in any
    /// assertion that only checks the events it names.
    #[test]
    fn a_final_record_without_a_trailing_newline_is_still_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-unterminated";
        let records = (0..525)
            .map(|index| {
                format!(
                    "{{\"type\":\"user\",\"uuid\":\"u-{index}\",\"sessionId\":\"{session_id}\",\
                     \"cwd\":\"/work/app\",\"timestamp\":\"2026-08-31T10:00:00Z\",\
                     \"message\":{{\"role\":\"user\",\"content\":\"synthetic cloud prompt {index}\"}}}}"
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !records.ends_with('\n'),
            "the fixture must not be terminated"
        );
        let transcript = seed_claude_transcript(dir.path(), session_id, records.as_bytes());
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let (result, result_records) =
            hydrate_counting_records(&db, &options("claude", session_id), dir.path());
        assert_eq!(result.evidence.prompts, 525);
        assert_eq!(session_event_snapshot(&db, session_id).len(), 525);
        // The whole file was read once by each walk, trailing record included.
        // Records only: a pass also hashes bounded windows to validate its
        // cursor, and those bytes are in `bytes_read` too. Reported by the
        // code that spends them, so this stays an exact statement about the
        // records.
        assert_eq!(result_records, 2 * records.len() as i64);

        // A pass that changes nothing must not duplicate the record, and the
        // cursor must sit at the end of the file so an untouched transcript is
        // skipped outright rather than re-read for its unterminated tail.
        assert_eq!(
            stored_cursor(&db, session_id).committed_offset(),
            records.len() as i64
        );
        let again =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert_eq!(again.status, "unchanged");
        // Not zero: skipping the file means validating that the bytes behind
        // its cursor are still the bytes on disk, which reads the two bounded
        // windows the cursor's hash covers. Recomputed from the window size
        // rather than pasted from a run.
        assert_eq!(
            again.bytes_read,
            2 * super::cursor::PREFIX_WINDOW_BYTES.min(records.len() as u64) as i64
        );
        assert_eq!(session_event_snapshot(&db, session_id).len(), 525);

        // When the newline and another record finally arrive, the pass rewinds
        // to the record it read unterminated rather than resuming after it, so
        // the completed line is re-read and upserted rather than skipped.
        let tail = format!(
            "\n{{\"type\":\"user\",\"uuid\":\"u-525\",\"sessionId\":\"{session_id}\",\
             \"cwd\":\"/work/app\",\"timestamp\":\"2026-08-31T10:01:00Z\",\
             \"message\":{{\"role\":\"user\",\"content\":\"synthetic cloud prompt 525\"}}}}\n"
        );
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        write!(file, "{tail}").unwrap();
        drop(file);
        let (_grown, grown_records) =
            hydrate_counting_records(&db, &options("claude", session_id), dir.path());
        assert_eq!(session_event_snapshot(&db, session_id).len(), 526);
        // The re-read covers the previously unterminated record plus the new
        // one, and nothing before it.
        let last_record_len = records.len() as i64 - (records.rfind('\n').unwrap() as i64 + 1);
        // Records only; the bounded validation windows are counted too and
        // reported by the code that spends them.
        assert_eq!(grown_records, 2 * (last_record_len + tail.len() as i64));
    }

    /// A partial line is still withheld: the distinction is whether it parses.
    #[test]
    fn a_half_written_final_record_is_withheld_until_it_parses() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-half-written";
        let complete = format!(
            "{{\"type\":\"user\",\"uuid\":\"u-1\",\"sessionId\":\"{session_id}\",\
             \"cwd\":\"/work/app\",\"timestamp\":\"2026-08-31T10:00:00Z\",\
             \"message\":{{\"role\":\"user\",\"content\":\"first\"}}}}\n"
        );
        let transcript = seed_claude_transcript(dir.path(), session_id, complete.as_bytes());
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);
        hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert_eq!(session_event_snapshot(&db, session_id).len(), 1);

        // Half of a record, with no newline. It is not JSON, so it is not
        // evidence of anything yet.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        write!(file, "{{\"type\":\"user\",\"uuid\":\"u-2\",\"sessionI").unwrap();
        drop(file);
        hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert_eq!(session_event_snapshot(&db, session_id).len(), 1);

        // The rest of it arrives.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        writeln!(
            file,
            "d\":\"{session_id}\",\"cwd\":\"/work/app\",\"timestamp\":\"2026-08-31T10:00:01Z\",\
             \"message\":{{\"role\":\"user\",\"content\":\"second\"}}}}"
        )
        .unwrap();
        drop(file);
        hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        let uids: Vec<String> = session_event_snapshot(&db, session_id)
            .into_iter()
            .map(|row| row.0)
            .collect();
        assert_eq!(uids, vec!["u-1:0".to_string(), "u-2:0".to_string()]);
    }

    /// A message left unfinished by a writer that then stopped must not be
    /// held back forever.
    ///
    /// Deferral is a bet that the provider will finish the message. When the
    /// file stops changing the bet has lost, and holding the records back
    /// again on every pass would lose the message permanently. A pass that
    /// finds the file byte-for-byte where its cursor left it indexes what it
    /// held.
    #[test]
    fn an_abandoned_in_progress_message_is_indexed_once_the_writer_stops() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = fs::read(fixture("incomplete-then-complete.jsonl")).unwrap();
        let transcript = seed_claude_transcript(dir.path(), INCOMPLETE_SESSION, &bytes);
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", INCOMPLETE_SESSION, Some(&transcript));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("claude", INCOMPLETE_SESSION), dir.path())
                .unwrap();
        assert_eq!(
            diagnostic(&first, "HYDRATION_IN_PROGRESS_MESSAGES").map(|held| held.records_parsed),
            Some(Some(1))
        );
        assert!(!session_event_snapshot(&db, INCOMPLETE_SESSION)
            .iter()
            .any(|row| row.0.starts_with("u-asst-2")));

        // Nothing appended, but a model that pauses between streamed records
        // has not stopped writing, so an immediate second pass keeps waiting.
        let paused =
            hydrate_session_at_with_home(&db, &options("claude", INCOMPLETE_SESSION), dir.path())
                .unwrap();
        assert!(
            diagnostic(&paused, "HYDRATION_IN_PROGRESS_MESSAGES").is_some(),
            "a pause shorter than the grace window is not an abandoned message"
        );
        assert!(!session_event_snapshot(&db, INCOMPLETE_SESSION)
            .iter()
            .any(|row| row.0.starts_with("u-asst-2")));

        // Once the file has been still for longer than the grace window the
        // writer really is gone, and the held records are released. A session
        // with records still held is never reported `unchanged`, which is what
        // lets this pass run at all.
        age_past_grace(&db, &session_cursor_key(INCOMPLETE_SESSION));
        let second =
            hydrate_session_at_with_home(&db, &options("claude", INCOMPLETE_SESSION), dir.path())
                .unwrap();
        assert_ne!(second.status, "unchanged");
        assert!(diagnostic(&second, "HYDRATION_IN_PROGRESS_MESSAGES").is_none());
        assert!(session_event_snapshot(&db, INCOMPLETE_SESSION)
            .iter()
            .any(|row| row.0 == "u-asst-2:0"));
        assert_eq!(
            stored_cursor(&db, INCOMPLETE_SESSION).committed_offset(),
            bytes.len() as i64
        );

        // And now it really is unchanged.
        let third =
            hydrate_session_at_with_home(&db, &options("claude", INCOMPLETE_SESSION), dir.path())
                .unwrap();
        assert_eq!(third.status, "unchanged");
        // The cursor covers the whole file, so its validation window is the
        // file, read twice: the first window and the last are the same bytes.
        assert_eq!(
            third.bytes_read,
            2 * super::cursor::PREFIX_WINDOW_BYTES.min(bytes.len() as u64) as i64
        );
    }

    /// A Codex rollout's identity comes from its first record, so a hydration
    /// that only needs the tail must not read the head-to-tail file.
    ///
    /// The twin of the Claude metadata scan, and hidden the same way:
    /// `read_codex_session_meta` did `fs::read_to_string` and then took
    /// `lines().next()`, so the call looked bounded at the call site while a
    /// kilobyte appended to a large rollout still read all of it — and
    /// `bytes_read` reported the kilobyte, because it counted the cursor's
    /// work rather than the hydration's.
    #[test]
    fn a_codex_append_does_not_re_read_the_rollout_for_its_identity() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join(".codex/sessions/2026/08/31");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-big.jsonl");
        let meta_line = "{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",\
                         \"payload\":{\"id\":\"big\",\"cwd\":\"/work/app\"}}\n";
        let mut body = String::from(meta_line);
        for index in 0..2000 {
            body.push_str(&format!(
                "{{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",\
                 \"payload\":{{\"type\":\"agent_message\",\"message\":\"answer {index} {}\"}}}}\n",
                "z".repeat(512)
            ));
        }
        body.push_str(
            "{\"timestamp\":\"2026-08-31T10:00:02Z\",\"type\":\"event_msg\",\
             \"payload\":{\"type\":\"task_complete\",\"turn_id\":\"t1\"}}\n",
        );
        fs::write(&rollout, &body).unwrap();
        let size = body.len() as i64;
        assert!(size > 1_000_000, "the rollout must dwarf the append");

        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "big", Some(&rollout));
        drop(conn);

        let (first, first_records) =
            hydrate_counting_records(&db, &options("codex", "big"), dir.path());
        assert_eq!(first.status, "hydrated");
        // The whole rollout, plus its `session_meta` record read twice from
        // the head: once to stamp the source, once to identify it for
        // ingestion. One record either way, never the file.
        let head_reads = 2 * meta_line.len() as i64;
        // Records only: a pass also hashes bounded windows to validate its
        // cursor, and those bytes are in `bytes_read` too. Reported by the
        // code that spends them, so this stays an exact statement about the
        // records.
        assert_eq!(first_records, size + head_reads);

        let addition = format!(
            "{{\"timestamp\":\"2026-08-31T10:00:03Z\",\"type\":\"event_msg\",\
             \"payload\":{{\"type\":\"agent_message\",\"message\":\"{}\"}}}}\n\
             {{\"timestamp\":\"2026-08-31T10:00:04Z\",\"type\":\"event_msg\",\
             \"payload\":{{\"type\":\"task_complete\",\"turn_id\":\"t2\"}}}}\n",
            "y".repeat(900)
        );
        let mut file = fs::OpenOptions::new().append(true).open(&rollout).unwrap();
        write!(file, "{addition}").unwrap();
        drop(file);

        let (appended, appended_records) =
            hydrate_counting_records(&db, &options("codex", "big"), dir.path());
        // The append, plus the same two head records. Not the rollout.
        // Records only; the bounded validation windows are counted too and
        // reported by the code that spends them.
        assert_eq!(appended_records, addition.len() as i64 + head_reads);
        // Positive control: the counter is capable of reporting the whole
        // file, and did so on the first pass, so a bounded second reading is
        // a fact about the read rather than about the counter.
        assert!(first_records > 100 * appended_records);
        // Totals, validation included, still shrink — though not by that
        // factor: validation is a constant per pass, so on a small append it
        // is most of what the pass reads. That is the trade, and stating a
        // ratio here would be asserting something the design does not claim.
        assert!(first.bytes_read > appended.bytes_read);
    }

    /// A message held back at byte zero is still released once the writer
    /// stops.
    ///
    /// Quiescence was only computed for a saved cursor whose offset was above
    /// zero, which reads "the cursor is at the start" as "there is no cursor".
    /// A transcript whose *first* record is the unfinished one therefore
    /// committed offset zero, and every later pass ignored the cursor, never
    /// saw the file go quiet, and deferred the same record again — while the
    /// held-records check kept forcing those passes to run. The message was
    /// never indexed and the session never became `unchanged`.
    #[test]
    fn a_message_held_at_offset_zero_is_still_released_when_the_writer_stops() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-zero-offset";
        let only_record = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"a-1\",\"cwd\":\"/work/app\",\
             \"type\":\"assistant\",\"message\":{{\"id\":\"msg_only\",\"role\":\"assistant\",\
             \"stop_reason\":null,\"content\":[{{\"type\":\"text\",\"text\":\"streaming\"}}]}},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
        );
        let transcript = seed_claude_transcript(dir.path(), session_id, only_record.as_bytes());
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert!(diagnostic(&first, "HYDRATION_IN_PROGRESS_MESSAGES").is_some());
        assert_eq!(session_event_snapshot(&db, session_id).len(), 0);
        assert_eq!(stored_cursor(&db, session_id).committed_offset(), 0);

        // Nothing changes, and the writer has been still long enough to say
        // so. The point of this test is the offset, not the window.
        age_past_grace(&db, &session_cursor_key(session_id));
        let second =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert_ne!(second.status, "unchanged");
        assert_eq!(
            session_event_snapshot(&db, session_id).len(),
            1,
            "a message held at offset zero must still be released"
        );
        assert_eq!(
            stored_cursor(&db, session_id).committed_offset(),
            only_record.len() as i64
        );

        // And it settles rather than re-running forever.
        let third =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert_eq!(third.status, "unchanged");
    }

    /// An unterminated trailing record is still subject to deferral.
    ///
    /// The unterminated-tail rule was added so a complete transcript's last
    /// record is not lost, and it indexed the record as soon as it parsed —
    /// without asking whether the record belonged to a message still being
    /// written. That skipped deferral exactly where a file is most likely to
    /// be mid-write: the tail. The record was written out, `in_progress`
    /// stayed empty, and no later pass held or completed it.
    #[test]
    fn an_unterminated_in_progress_record_is_deferred_not_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-unterminated-live";
        let prompt = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/work/app\",\
             \"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"go\"}},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
        );
        // Complete JSON, no trailing newline, and still streaming.
        let streaming = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"a-1\",\"cwd\":\"/work/app\",\
             \"type\":\"assistant\",\"message\":{{\"id\":\"msg_live\",\"role\":\"assistant\",\
             \"stop_reason\":null,\"content\":[{{\"type\":\"text\",\"text\":\"thinking\"}}]}},\
             \"timestamp\":\"2026-08-31T10:00:01Z\"}}"
        );
        let transcript = seed_claude_transcript(
            dir.path(),
            session_id,
            format!("{prompt}{streaming}").as_bytes(),
        );
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        let held = diagnostic(&first, "HYDRATION_IN_PROGRESS_MESSAGES")
            .expect("an unterminated streaming record is held, not written");
        assert!(held.message.contains("msg_live"), "{}", held.message);
        let uids: Vec<String> = session_event_snapshot(&db, session_id)
            .into_iter()
            .map(|row| row.0)
            .collect();
        // Positive control: the prompt before it *was* indexed, so this is
        // deferral rather than a pass that read nothing.
        assert_eq!(uids, vec!["u-1:0".to_string()]);
        assert_eq!(
            stored_cursor(&db, session_id).committed_offset(),
            prompt.len() as i64,
            "the cursor must not advance past a record it held"
        );

        // The writer stops without ever terminating the line, and stays
        // stopped for longer than the grace window.
        age_past_grace(&db, &session_cursor_key(session_id));
        let second =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert!(diagnostic(&second, "HYDRATION_IN_PROGRESS_MESSAGES").is_none());
        let uids: Vec<String> = session_event_snapshot(&db, session_id)
            .into_iter()
            .map(|row| row.0)
            .collect();
        assert_eq!(uids, vec!["a-1:0".to_string(), "u-1:0".to_string()]);
    }

    /// A sidecar that held a message back gets the pass it needs to release
    /// it, even though nothing about the session changed.
    ///
    /// Sidecars keep their parser state in their own locator-keyed cursors.
    /// The unchanged shortcut consulted only the session's cursor, so a child
    /// message deferred by a sidecar stayed unindexed for as long as nothing
    /// else about the session moved.
    #[test]
    fn a_sidecar_holding_a_message_still_gets_a_pass_to_release_it() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-sidecar-held";
        let transcript = seed_claude_transcript(
            dir.path(),
            session_id,
            format!(
                "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/work/app\",\
                 \"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"parent\"}},\
                 \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
            )
            .as_bytes(),
        );
        let sidecar = transcript.parent().unwrap().join("agent-child.jsonl");
        fs::write(
            &sidecar,
            format!(
                "{{\"sessionId\":\"{session_id}\",\"agentId\":\"child-1\",\"isSidechain\":true,\
                 \"uuid\":\"s-1\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\
                 \"message\":{{\"id\":\"msg_child\",\"role\":\"assistant\",\"stop_reason\":null,\
                 \"content\":[{{\"type\":\"text\",\"text\":\"child streaming\"}}]}},\
                 \"timestamp\":\"2026-08-31T10:00:01Z\"}}\n"
            ),
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert!(diagnostic(&first, "HYDRATION_IN_PROGRESS_MESSAGES").is_some());
        assert_eq!(session_event_snapshot(&db, "child-1").len(), 0);
        // Positive control: the parent's own record did land, so the pass ran.
        assert_eq!(session_event_snapshot(&db, session_id).len(), 1);

        // Nothing changes anywhere. The shortcut must not fire while the
        // sidecar is still holding records, and the sidecar's own cursor is
        // what has to age out before they are released.
        let locator = sidecar.to_string_lossy().to_string();
        age_past_grace(
            &db,
            &CursorKey::Locator {
                source: "claude",
                locator: &locator,
            },
        );
        let second =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert_ne!(
            second.status, "unchanged",
            "a sidecar holding records must still get a pass"
        );
        assert_eq!(
            session_event_snapshot(&db, "child-1")
                .into_iter()
                .map(|row| row.0)
                .collect::<Vec<_>>(),
            vec!["s-1:0".to_string()]
        );

        let third =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert_eq!(third.status, "unchanged");
    }

    /// Enumerating sidecars resumes their metadata walks.
    #[test]
    fn a_growing_sidecar_is_not_re_identified_from_byte_zero() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-sidecar-scan";
        let transcript = seed_claude_transcript(
            dir.path(),
            session_id,
            format!(
                "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/work/app\",\
                 \"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"parent\"}},\
                 \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
            )
            .as_bytes(),
        );
        let sidecar = transcript.parent().unwrap().join("agent-child.jsonl");
        let mut body = String::new();
        let mut first_record_len = 0usize;
        for index in 0..2000 {
            let record = format!(
                "{{\"sessionId\":\"{session_id}\",\"agentId\":\"child-1\",\"isSidechain\":true,\
                 \"uuid\":\"s-{index}\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\
                 \"message\":{{\"id\":\"m-{index}\",\"role\":\"assistant\",\
                 \"stop_reason\":\"end_turn\",\"content\":\"{}\"}},\
                 \"timestamp\":\"2026-08-31T10:00:01Z\"}}\n",
                "z".repeat(512)
            );
            if index == 0 {
                first_record_len = record.len();
            }
            body.push_str(&record);
        }
        fs::write(&sidecar, &body).unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert!(first.bytes_read > body.len() as i64);

        let addition = format!(
            "{{\"sessionId\":\"{session_id}\",\"agentId\":\"child-1\",\"isSidechain\":true,\
             \"uuid\":\"s-tail\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\
             \"message\":{{\"id\":\"m-tail\",\"role\":\"assistant\",\"stop_reason\":\"end_turn\",\
             \"content\":\"{}\"}},\"timestamp\":\"2026-08-31T10:00:02Z\"}}\n",
            "y".repeat(400)
        );
        let mut file = fs::OpenOptions::new().append(true).open(&sidecar).unwrap();
        write!(file, "{addition}").unwrap();
        drop(file);

        let (_appended, appended_records) =
            hydrate_counting_records(&db, &options("claude", session_id), dir.path());
        // The sidecar's metadata walk and its record walk each read the
        // appended record, plus the one head record the child's evidence
        // needs. Nothing reads the sidecar's two thousand records again.
        // Records only: a pass also hashes bounded windows to validate its
        // cursor, and those bytes are in `bytes_read` too. Reported by the
        // code that spends them, so this stays an exact statement about the
        // records.
        assert_eq!(
            appended_records,
            2 * addition.len() as i64 + first_record_len as i64
        );
        assert!(first.bytes_read > 100 * appended_records);
    }

    /// Enumerating Codex children reads every sibling rollout's head record,
    /// and those bytes are part of what the hydration read.
    ///
    /// The accounting gap one level out from the last round: the reads that
    /// *find* the related sessions were missing from the counter, so a parent
    /// with ten candidates could report the root's head alone.
    #[test]
    fn codex_child_enumeration_reads_are_counted() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join(".codex/sessions/2026/08/31");
        fs::create_dir_all(&day).unwrap();
        let root = day.join("rollout-root.jsonl");
        fs::write(
            &root,
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"root\",\"cwd\":\"/work/app\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"root prompt\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:02Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"t1\"}}\n",
            ),
        )
        .unwrap();
        // Unrelated siblings. Their heads are read to discover they are not
        // children, which is work the hydration did either way.
        let mut sibling_head_bytes = 0i64;
        for index in 0..8 {
            let sibling = day.join(format!("rollout-other-{index}.jsonl"));
            let head = format!(
                "{{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",\
                 \"payload\":{{\"id\":\"other-{index}\",\"cwd\":\"/work/app\"}}}}\n"
            );
            let body = format!(
                "{head}{{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",\
                 \"payload\":{{\"type\":\"user_message\",\"message\":\"{}\"}}}}\n",
                "q".repeat(4096)
            );
            fs::write(&sibling, body).unwrap();
            sibling_head_bytes += head.len() as i64;
        }

        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "root", Some(&root));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        // Enumeration runs twice — once to stamp, once to ingest — and each
        // pass reads every sibling's head.
        let root_len = fs::metadata(&root).unwrap().len() as i64;
        assert!(
            first.bytes_read >= root_len + 2 * sibling_head_bytes,
            "bytes_read {} omits the {} bytes of sibling heads the enumeration read",
            first.bytes_read,
            2 * sibling_head_bytes
        );
        // Positive control: the siblings' bodies are far larger than their
        // heads, and none of that is counted, so this is the head reads being
        // included rather than the whole directory being read.
        let directory_bytes: i64 = (0..8)
            .map(|index| {
                fs::metadata(day.join(format!("rollout-other-{index}.jsonl")))
                    .unwrap()
                    .len() as i64
            })
            .sum();
        assert!(first.bytes_read < root_len + directory_bytes);
    }

    /// A Claude sidecar's head record and its metadata document are provider
    /// reads, and belong in the counter with everything else.
    #[test]
    fn claude_sidecar_evidence_reads_are_counted() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-evidence-bytes";
        let transcript = seed_claude_transcript(
            dir.path(),
            session_id,
            format!(
                "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/work/app\",\
                 \"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"parent\"}},\
                 \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
            )
            .as_bytes(),
        );
        let sidecar = transcript.parent().unwrap().join("agent-child.jsonl");
        let sidecar_body = format!(
            "{{\"sessionId\":\"{session_id}\",\"agentId\":\"child-1\",\"isSidechain\":true,\
             \"uuid\":\"s-1\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\
             \"message\":{{\"id\":\"m-1\",\"role\":\"assistant\",\"stop_reason\":\"end_turn\",\
             \"content\":\"child\"}},\"timestamp\":\"2026-08-31T10:00:01Z\"}}\n"
        );
        fs::write(&sidecar, &sidecar_body).unwrap();
        let meta_doc = "{\"agentType\":\"Plan\",\"description\":\"plan it\",\
                        \"toolUseId\":\"toolu_1\",\"spawnDepth\":1,\"model\":\"m\"}";
        fs::write(sidecar.with_extension("meta.json"), meta_doc).unwrap();

        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        let parent_len = fs::metadata(&transcript).unwrap().len() as i64;
        // The metadata document is read once while building the child's
        // evidence, on top of the two walks over each transcript and the
        // sidecar's head record.
        let floor = 2 * parent_len + 2 * sidecar_body.len() as i64 + meta_doc.len() as i64;
        assert!(
            result.bytes_read >= floor,
            "bytes_read {} omits the sidecar head or metadata document (floor {floor})",
            result.bytes_read
        );
        // Positive control: the child really was described from that
        // document, so the bytes were spent on something.
        let agent_type: Option<String> = open_db(&db)
            .unwrap()
            .query_row(
                "SELECT child_agent_type FROM session_relationships WHERE source='claude'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(agent_type.as_deref(), Some("Plan"));
    }

    /// One enormous record must not cost the file's size in memory.
    ///
    /// `read_until` extends its buffer until a newline or EOF, so the Claude
    /// deferral cap could only ever notice an allocation that had already
    /// happened. Both readers used it, so a transcript with one 500 MiB
    /// record made the reader allocate 500 MiB whatever the caps said. The
    /// ceiling is now on the reader.
    #[test]
    fn a_record_over_the_ceiling_is_skipped_rather_than_held() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-oversized";
        // Under the ceiling, valid, and indexed: the positive control that
        // makes "skipped" a fact about the size rather than about the reader.
        let small = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/w\",\"type\":\"user\",\
             \"message\":{{\"role\":\"user\",\"content\":\"small\"}},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
        );
        let huge = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-2\",\"cwd\":\"/w\",\"type\":\"user\",\
             \"message\":{{\"role\":\"user\",\"content\":\"{}\"}},\
             \"timestamp\":\"2026-08-31T10:00:01Z\"}}\n",
            "z".repeat(super::cursor::MAX_RECORD_BYTES as usize)
        );
        let after = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-3\",\"cwd\":\"/w\",\"type\":\"user\",\
             \"message\":{{\"role\":\"user\",\"content\":\"after\"}},\
             \"timestamp\":\"2026-08-31T10:00:02Z\"}}\n"
        );
        let transcript = seed_claude_transcript(
            dir.path(),
            session_id,
            format!("{small}{huge}{after}").as_bytes(),
        );
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let before = peak_rss_bytes();
        let result =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        let growth = peak_rss_bytes().saturating_sub(before);

        // The records either side of it are indexed; the oversized one is not.
        let uids: Vec<String> = session_event_snapshot(&db, session_id)
            .into_iter()
            .map(|row| row.0)
            .collect();
        assert_eq!(uids, vec!["u-1:0".to_string(), "u-3:0".to_string()]);

        let skipped = diagnostic(&result, "HYDRATION_OVERSIZED_RECORDS")
            .expect("a skipped record is reported, not silently dropped");
        assert_eq!(skipped.records_parsed, Some(1));

        // Never held: the record is 16 MiB and the ceiling is the only thing
        // between it and the allocator.
        assert!(
            growth < super::cursor::MAX_RECORD_BYTES,
            "hydration grew peak RSS by {growth} bytes over a {} byte record",
            super::cursor::MAX_RECORD_BYTES
        );
    }

    /// The same ceiling, on a record the file ends inside.
    #[test]
    fn an_oversized_unterminated_record_leaves_the_cursor_where_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-oversized-tail";
        let small = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/w\",\"type\":\"user\",\
             \"message\":{{\"role\":\"user\",\"content\":\"small\"}},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
        );
        // No newline: the file ends in the middle of a record that is already
        // past the ceiling.
        let huge_tail = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-2\",\"cwd\":\"/w\",\"content\":\"{}\"",
            "z".repeat(super::cursor::MAX_RECORD_BYTES as usize)
        );
        let transcript = seed_claude_transcript(
            dir.path(),
            session_id,
            format!("{small}{huge_tail}").as_bytes(),
        );
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let result =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert!(diagnostic(&result, "HYDRATION_OVERSIZED_RECORDS").is_some());
        // Positive control: the complete record before it landed.
        assert_eq!(session_event_snapshot(&db, session_id).len(), 1);
        // The cursor stops before the record the file ends inside, so a writer
        // still producing it is not skipped past.
        assert_eq!(
            stored_cursor(&db, session_id).committed_offset(),
            small.len() as i64
        );
    }

    /// Bytes that are not valid UTF-8 are a malformed record, not text to
    /// repair.
    ///
    /// `from_utf8_lossy` turned an invalid byte inside a JSON string into
    /// U+FFFD while leaving the syntax valid, so a corrupted record parsed
    /// cleanly and was indexed as though the replacement character were what
    /// the provider wrote.
    #[test]
    fn a_record_with_invalid_utf8_is_skipped_rather_than_repaired() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-invalid-utf8";
        let mut bytes = Vec::new();
        // Valid, and multi-byte, so the control proves strict decoding did not
        // simply reject everything non-ASCII.
        bytes.extend_from_slice(
            format!(
                "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/w\",\"type\":\"user\",\
                 \"message\":{{\"role\":\"user\",\"content\":\"héllo wörld — ok\"}},\
                 \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
            )
            .as_bytes(),
        );
        // The invalid byte sits *inside* a string, so lossy decoding produces
        // a record that is still valid JSON — which is the whole danger. A
        // record that would be malformed either way proves nothing about how
        // it was decoded, and an earlier draft of this test made exactly that
        // mistake and passed against the defect.
        let prefix = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-2\",\"cwd\":\"/w\",\"type\":\"user\",\
             \"message\":{{\"role\":\"user\",\"content\":\"bad"
        );
        let suffix = "\"},\"timestamp\":\"2026-08-31T10:00:01Z\"}\n";
        bytes.extend_from_slice(prefix.as_bytes());
        bytes.push(0xFF);
        bytes.extend_from_slice(suffix.as_bytes());
        // Prove the fixture is what the test claims: replacing the invalid
        // byte the way `from_utf8_lossy` would gives a record that parses.
        let repaired = format!("{prefix}{}{suffix}", char::REPLACEMENT_CHARACTER);
        assert!(
            serde_json::from_str::<Value>(repaired.trim_end()).is_ok(),
            "the lossy form of this record must be valid JSON, or the test \
             cannot tell strict decoding from a parse failure"
        );
        let transcript = seed_claude_transcript(dir.path(), session_id, &bytes);
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        let rows = session_event_snapshot(&db, session_id);
        let uids: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
        assert_eq!(
            uids,
            vec!["u-1:0"],
            "a record with an invalid byte must be skipped, not repaired"
        );
        // Positive control: the multi-byte record survived intact, so strict
        // decoding rejected the invalid bytes rather than everything.
        assert!(rows[0].3.contains("héllo wörld — ok"), "{:?}", rows[0]);
        // And nothing was written with a replacement character.
        assert!(!rows.iter().any(|row| row.3.contains('\u{FFFD}')));
    }

    /// A Codex rollout's unterminated tail was read, so it is counted.
    ///
    /// `bytes_read` was computed from the reader's position, which by design
    /// does not advance over a tail the pass declines to commit — so the bytes
    /// `next_line` had already consumed went unreported. The same omission
    /// this counter has had to be corrected for three times elsewhere.
    #[test]
    fn a_codex_unterminated_tail_is_counted_in_bytes_read() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join(".codex/sessions/2026/08/31");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-tail.jsonl");
        let meta_line = "{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",\
                         \"payload\":{\"id\":\"tail\",\"cwd\":\"/work/app\"}}\n";
        let complete = format!(
            "{meta_line}{{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",\
             \"payload\":{{\"type\":\"user_message\",\"message\":\"first\"}}}}\n\
             {{\"timestamp\":\"2026-08-31T10:00:02Z\",\"type\":\"event_msg\",\
             \"payload\":{{\"type\":\"task_complete\",\"turn_id\":\"t1\"}}}}\n"
        );
        fs::write(&rollout, &complete).unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "tail", Some(&rollout));
        drop(conn);

        let (_first, first_records) =
            hydrate_counting_records(&db, &options("codex", "tail"), dir.path());
        // Records only: a pass also hashes bounded windows to validate its
        // cursor, and those bytes are in `bytes_read` too. Reported by the
        // code that spends them, so this stays an exact statement about the
        // records.
        assert_eq!(
            first_records,
            complete.len() as i64 + 2 * meta_line.len() as i64
        );

        // A half-written record arrives with no newline. The pass reads it,
        // declines to commit it, and must still report having read it.
        let tail = "{\"timestamp\":\"2026-08-31T10:00:03Z\",\"type\":\"event_ms";
        let mut file = fs::OpenOptions::new().append(true).open(&rollout).unwrap();
        write!(file, "{tail}").unwrap();
        drop(file);

        let (_appended, appended_records) =
            hydrate_counting_records(&db, &options("codex", "tail"), dir.path());
        // Records only; the bounded validation windows are counted too and
        // reported by the code that spends them.
        assert_eq!(
            appended_records,
            tail.len() as i64 + 2 * meta_line.len() as i64,
            "an unterminated tail the reader consumed must appear in bytes_read"
        );
        // Positive control: the tail was not committed, so the cursor has not
        // advanced over it and the next pass will read it again.
        assert!(appended_records > 2 * meta_line.len() as i64);
    }

    /// A rewrite with the same size and mtime is still a rewrite, and the
    /// targeted hydration shortcut has to notice it too.
    ///
    /// The global sync walk got this check in round 3, through
    /// `transcript_unchanged`. Hydration's own shortcut compares the
    /// mtime-and-size stamp and returns *before* `TranscriptReader::open`, so
    /// the prefix hash the cursor stores never got a say and a rewritten file
    /// was served from rows built out of bytes that no longer exist.
    #[test]
    fn a_same_stat_rewrite_is_not_served_by_the_unchanged_shortcut() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-shortcut-rewrite";
        let original = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/w\",\"type\":\"user\",\
             \"message\":{{\"role\":\"user\",\"content\":\"aaaaa\"}},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
        );
        let transcript = seed_claude_transcript(dir.path(), session_id, original.as_bytes());
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        let stamped = fs::metadata(&transcript).unwrap().modified().unwrap();

        // Positive control: an untouched file still takes the shortcut. Without
        // it, "the rewrite was not served from the cursor" would also be
        // satisfied by a check that never returns `unchanged` at all.
        let untouched =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert_eq!(
            untouched.status, "unchanged",
            "an untouched transcript must still be skipped, or this test proves nothing"
        );

        // Same length, different bytes, timestamp put back — what a writer
        // that preserves mtime, or a coarse filesystem clock, produces free.
        let rewritten = original.replace("aaaaa", "bbbbb");
        assert_eq!(rewritten.len(), original.len());
        fs::write(&transcript, &rewritten).unwrap();
        fs::File::open(&transcript)
            .unwrap()
            .set_modified(stamped)
            .unwrap();
        let metadata = fs::metadata(&transcript).unwrap();
        assert_eq!(metadata.len() as usize, original.len());
        assert_eq!(metadata.modified().unwrap(), stamped);

        let after =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert_ne!(
            after.status, "unchanged",
            "a same-size, same-mtime rewrite must not be served from the cursor"
        );
        let texts: Vec<String> = session_event_snapshot(&db, session_id)
            .into_iter()
            .map(|row| row.3)
            .collect();
        assert!(
            texts.iter().any(|text| text.contains("bbbbb")),
            "the rewritten bytes must be what is served, got {texts:?}"
        );
        assert!(
            !texts.iter().any(|text| text.contains("aaaaa")),
            "evidence from the replaced bytes must not survive, got {texts:?}"
        );
    }

    /// An unchanged pass still read the provider files that told it nothing
    /// changed, and says so.
    ///
    /// `source_snapshot` reads the Codex root's `session_meta` and one head
    /// record from every sibling rollout *before* the shortcut is evaluated —
    /// that is how the stamp is built. Returning an empty outcome reported
    /// `bytesRead: 0` for a pass that opened nine files, which is the same
    /// well-formed zero as "nothing was read".
    #[test]
    fn an_unchanged_pass_reports_the_provider_reads_it_made() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join(".codex/sessions/2026/08/31");
        fs::create_dir_all(&day).unwrap();
        let root = day.join("rollout-root.jsonl");
        let root_body = concat!(
            "{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"root\",\"cwd\":\"/work/app\"}}\n",
            "{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"root prompt\"}}\n",
            "{\"timestamp\":\"2026-08-31T10:00:02Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"t1\"}}\n",
        );
        fs::write(&root, root_body).unwrap();
        let mut sibling_head_bytes = 0i64;
        let mut directory_bytes = 0i64;
        for index in 0..8 {
            let sibling = day.join(format!("rollout-other-{index}.jsonl"));
            let head = format!(
                "{{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",\
                 \"payload\":{{\"id\":\"other-{index}\",\"cwd\":\"/work/app\"}}}}\n"
            );
            let body = format!(
                "{head}{{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",\
                 \"payload\":{{\"type\":\"user_message\",\"message\":\"{}\"}}}}\n",
                "q".repeat(4096)
            );
            fs::write(&sibling, body.as_bytes()).unwrap();
            sibling_head_bytes += head.len() as i64;
            directory_bytes += body.len() as i64;
        }

        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "root", Some(&root));
        drop(conn);

        let first =
            hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        let second =
            hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        assert_eq!(
            second.status, "unchanged",
            "the second pass must take the shortcut, or this test measures the wrong pass"
        );
        assert!(
            second.bytes_read >= sibling_head_bytes,
            "an unchanged pass read {sibling_head_bytes} bytes of sibling heads to \
             decide it was unchanged, and reported bytes_read {}",
            second.bytes_read
        );
        // Positive control: this is the head reads being counted, not the pass
        // re-reading the transcripts it decided not to read.
        assert!(
            second.bytes_read < first.bytes_read,
            "an unchanged pass must still read less than the pass that indexed \
             the session: {} vs {}",
            second.bytes_read,
            first.bytes_read
        );
        assert!(second.bytes_read < directory_bytes);
    }

    /// A record drained for passing the ceiling was still read, so its bytes
    /// are counted — without the cursor advancing over it.
    ///
    /// `drain_oversized_record` reaching EOF updated neither the position nor
    /// the tail, so a 16 MiB tail walked past in fixed-size chunks was absent
    /// from `bytes_read` entirely: the pass reported roughly the length of the
    /// one small record before it.
    #[test]
    fn an_oversized_tail_is_counted_in_bytes_read() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-oversized-tail-bytes";
        let small = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/w\",\"type\":\"user\",\
             \"message\":{{\"role\":\"user\",\"content\":\"small\"}},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
        );
        // No newline: the file ends inside a record already past the ceiling.
        let huge_tail = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-2\",\"cwd\":\"/w\",\"content\":\"{}\"",
            "z".repeat(super::cursor::MAX_RECORD_BYTES as usize)
        );
        let transcript = seed_claude_transcript(
            dir.path(),
            session_id,
            format!("{small}{huge_tail}").as_bytes(),
        );
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        let (_result, result_records) =
            hydrate_counting_records(&db, &options("claude", session_id), dir.path());
        // A Claude transcript is walked twice — once for identity and
        // metadata, once to index records — and each walk drains the tail, so
        // each walk reports it. Counting a read once per read is what makes
        // this counter mean anything.
        // Records only: a pass also hashes bounded windows to validate its
        // cursor, and those bytes are in `bytes_read` too. Reported by the
        // code that spends them, so this stays an exact statement about the
        // records.
        assert_eq!(
            result_records,
            2 * (small.len() + huge_tail.len()) as i64,
            "the drained tail was read in chunks and must be counted"
        );
        // Positive control: counting it did not commit it. The cursor stops
        // before the record the file ends inside, so a writer still producing
        // it is not skipped past.
        assert_eq!(
            stored_cursor(&db, session_id).committed_offset(),
            small.len() as i64
        );
    }

    /// The quiet-since clock and the stat it is paired with come from the same
    /// instant.
    ///
    /// The clock was stamped when the reader opened and the size and mtime it
    /// is compared against were taken at `commit`, so the window covered the
    /// whole pass. A full re-parse of a large live transcript takes longer
    /// than two minutes, and the next pass then found a tail that had settled
    /// seconds ago already "still for the window" — and released a message
    /// that was still being streamed.
    #[test]
    fn the_quiet_clock_restarts_when_the_file_changes_during_the_pass() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grace.jsonl");
        fs::write(&path, "first\n").unwrap();

        // A pass whose walk outlasts the grace window. Ageing the open stamp
        // stands in for the elapsed walk; sleeping would take two minutes to
        // prove the same thing.
        let mut reader = super::cursor::TranscriptReader::open(&path, None, None).unwrap();
        reader.age_unchanged_since_for_test(super::cursor::QUIESCENT_GRACE_MS + 1);

        // The writer appends while the pass is still walking, so the stat
        // `commit` records is newer than the clock the pass opened with.
        fs::write(&path, "first\nsecond\n").unwrap();
        // An append is not a rewrite, so this pass still publishes; what it
        // must not carry forward is the clock it opened with.
        let super::cursor::CommitOutcome::Published(committed) = reader.commit(6).unwrap() else {
            panic!("an append during the pass still commits");
        };
        assert!(
            now_ms().saturating_sub(committed.unchanged_since_ms)
                < super::cursor::QUIESCENT_GRACE_MS,
            "the clock must be stamped from the same instant as the stat it is paired with"
        );

        let next = super::cursor::TranscriptReader::open(&path, Some(&committed), None).unwrap();
        assert!(
            !next.quiesced(),
            "a file that changed during the pass has not been still for the window"
        );

        // Positive control: a file that really has been still for the window
        // still releases, so this is the clock being paired with its stat and
        // not quiescence being switched off.
        let mut quiet = committed.clone();
        quiet.unchanged_since_ms = now_ms() - super::cursor::QUIESCENT_GRACE_MS - 1;
        let released = super::cursor::TranscriptReader::open(&path, Some(&quiet), None).unwrap();
        assert!(
            released.quiesced(),
            "a genuinely quiescent file must still release what it held"
        );
    }

    /// A Codex child rollout is folded into the parent's stamp, so it needs
    /// the same prefix validation the parent and the Claude sidecars get.
    ///
    /// Round 5 gave the skip path a bounded window check and reached the
    /// session's own transcript and the Claude sidecars. Codex children were
    /// folded into the stamp with `file_stamp` alone, and they carry
    /// locator-keyed cursors of their own — so a same-size, same-mtime rewrite
    /// of a child was still served from the replaced bytes.
    #[test]
    fn a_same_stat_rewrite_of_a_codex_child_is_not_served_from_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join(".codex/sessions/2026/08/31");
        fs::create_dir_all(&day).unwrap();
        let root = day.join("rollout-root.jsonl");
        fs::write(
            &root,
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"root\",\"cwd\":\"/work/app\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"root prompt\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:02Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"t1\"}}\n",
            ),
        )
        .unwrap();
        let child = day.join("rollout-child.jsonl");
        let child_body = concat!(
            "{\"timestamp\":\"2026-08-31T10:00:03Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"child\",\"session_id\":\"root\",\"parent_thread_id\":\"root\",\"cwd\":\"/work/app\",\"thread_source\":\"subagent\",\"source\":{\"subagent\":{\"other\":\"guardian\"}}}}\n",
            "{\"timestamp\":\"2026-08-31T10:00:04Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"aaaaa\"}}\n",
            "{\"timestamp\":\"2026-08-31T10:00:05Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"t2\"}}\n",
        );
        fs::write(&child, child_body).unwrap();

        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "codex", "root", Some(&root));
        drop(conn);

        hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        let stamped = fs::metadata(&child).unwrap().modified().unwrap();

        // Positive control: nothing touched, so the shortcut is available and
        // fires. Without it, "the rewrite was not skipped" is also satisfied
        // by a check that never skips anything.
        let untouched =
            hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        assert_eq!(
            untouched.status, "unchanged",
            "an untouched parent and child must still be skipped"
        );

        let rewritten = child_body.replace("aaaaa", "bbbbb");
        assert_eq!(rewritten.len(), child_body.len());
        fs::write(&child, &rewritten).unwrap();
        fs::File::open(&child)
            .unwrap()
            .set_modified(stamped)
            .unwrap();
        let metadata = fs::metadata(&child).unwrap();
        assert_eq!(metadata.len() as usize, child_body.len());
        assert_eq!(metadata.modified().unwrap(), stamped);

        let after =
            hydrate_session_at_with_home(&db, &options("codex", "root"), dir.path()).unwrap();
        assert_ne!(
            after.status, "unchanged",
            "a same-size, same-mtime rewrite of a child must not be served from its cursor"
        );
    }

    /// A cursor may only vouch for the bytes its rows came from.
    ///
    /// The reader stats the file at open and again at commit, and reads
    /// records through a separate handle in between. Until now a stat that
    /// moved during the walk only restarted the quiescence clock: the cursor
    /// still stored the *new* stat and hashed the prefix from the file as it
    /// was at commit. A record rewritten in place mid-walk therefore left a
    /// row holding the old text and a cursor authenticating the new bytes —
    /// and the next pass validated that cursor and read nothing, so the stale
    /// row was permanent.
    #[test]
    fn a_rewrite_during_the_pass_is_not_blessed_by_the_cursor() {
        use super::cursor::{CommitOutcome, TranscriptReader};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rewritten.jsonl");
        let original = "{\"uuid\":\"u1\",\"text\":\"old\"}\n";
        fs::write(&path, original).unwrap();

        let mut reader = TranscriptReader::open(&path, None, None).unwrap();
        let mut line = String::new();
        reader.next_line(&mut line).unwrap();
        assert!(line.contains("old"), "the pass read the original bytes");

        // The provider rewrites that record in place — same length, so the
        // size cannot betray it, and a new mtime because something wrote.
        let rewritten = original.replace("old", "new");
        assert_eq!(rewritten.len(), original.len());
        fs::write(&path, &rewritten).unwrap();
        bump_mtime(&path);

        assert!(
            matches!(
                reader.commit(original.len() as u64).unwrap(),
                CommitOutcome::Superseded
            ),
            "a cursor must not authenticate bytes its rows did not come from"
        );

        // Positive control: an append during the pass leaves everything this
        // pass read where it was, so it still commits and still resumes.
        let mut reader = TranscriptReader::open(&path, None, None).unwrap();
        reader.next_line(&mut line).unwrap();
        let appended = format!("{rewritten}{{\"uuid\":\"u2\",\"text\":\"later\"}}\n");
        fs::write(&path, &appended).unwrap();
        bump_mtime(&path);
        let CommitOutcome::Published(file) = reader.commit(rewritten.len() as u64).unwrap() else {
            panic!("a plain append during the pass must still commit");
        };
        assert_eq!(file.offset, rewritten.len() as u64);
    }

    /// The whole path: a rewrite under a live pass records no cursor, and the
    /// pass that follows corrects the row.
    ///
    /// Driven through the incremental pass rather than a whole hydration,
    /// because a hydration walks the transcript twice and a hook that fired on
    /// the first walk would have rewritten the file *before* the second walk
    /// read it — a pass reading the new bytes correctly, which is not the
    /// defect. The first version of this test made exactly that mistake and
    /// reported no diagnostic for the right reason.
    #[test]
    fn a_transcript_rewritten_under_a_pass_is_read_again_and_corrected() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-rewritten-under-pass";
        let transcript = dir.path().join("rewritten-under-pass.jsonl");
        let original = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/w\",\"type\":\"user\",\
             \"message\":{{\"role\":\"user\",\"content\":\"old\"}},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
        );
        let rewritten = original.replace("\"content\":\"old\"", "\"content\":\"new\"");
        assert_eq!(rewritten.len(), original.len());
        fs::write(&transcript, original.as_bytes()).unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();

        // The writer rewrites the record in place after the records have been
        // parsed and before the cursor is written — the one moment no test can
        // otherwise reach. Once only: the pass that follows must find the file
        // still.
        let target = transcript.clone();
        let bytes = rewritten.clone();
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let armed = std::sync::Arc::clone(&fired);
        super::cursor::set_before_commit_hook_for_test(Some(Box::new(move |_committing| {
            if armed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            fs::write(&target, &bytes).unwrap();
            bump_mtime(&target);
        })));
        let during = crate::ingest::incremental::ingest_claude_transcript_at_locator(
            &conn,
            &transcript,
            None,
        )
        .unwrap();
        super::cursor::set_before_commit_hook_for_test(None);
        assert!(
            fired.load(std::sync::atomic::Ordering::SeqCst),
            "the rewrite must land between the read and the commit, or this test proves nothing"
        );

        assert!(
            during.superseded,
            "a pass whose bytes moved under it records nothing and says so"
        );
        let locator = transcript.to_string_lossy().to_string();
        let key = CursorKey::Locator {
            source: "claude",
            locator: &locator,
        };
        assert_eq!(
            load_cursor(&conn, &key).unwrap().committed_offset(),
            0,
            "nothing may be recorded about a pass whose bytes moved under it"
        );

        // The next pass reads the same region again and upserts over the row
        // the superseded pass left behind.
        let after = crate::ingest::incremental::ingest_claude_transcript_at_locator(
            &conn,
            &transcript,
            None,
        )
        .unwrap();
        assert!(!after.superseded, "the file is still now");
        assert_eq!(
            load_cursor(&conn, &key).unwrap().committed_offset(),
            rewritten.len() as i64,
            "and a pass that was not interrupted records its position"
        );
        drop(conn);
        let texts: Vec<String> = session_event_snapshot(&db, session_id)
            .into_iter()
            .map(|row| row.3)
            .collect();
        assert!(
            texts.iter().any(|text| text.contains("new")),
            "the corrected bytes must reach the rows, got {texts:?}"
        );
        assert!(
            !texts.iter().any(|text| text.contains("old")),
            "the row from the superseded pass must not survive, got {texts:?}"
        );
    }

    /// A rewrite that keeps the stat is the one a stat cannot see.
    ///
    /// The first version of this guard compared the window only when the size
    /// or mtime had moved — which leaves out exactly the rewrite nothing else
    /// would catch. The skip path already assumes writers restore timestamps;
    /// the commit path has to assume it too.
    #[test]
    fn a_rewrite_that_restores_the_stat_is_not_blessed_by_the_cursor() {
        use super::cursor::{CommitOutcome, TranscriptReader};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("same-stat.jsonl");
        let original = "{\"uuid\":\"u1\",\"text\":\"old\"}\n";
        fs::write(&path, original).unwrap();
        let opened_at = fs::metadata(&path).unwrap().modified().unwrap();

        let mut reader = TranscriptReader::open(&path, None, None).unwrap();
        let mut line = String::new();
        reader.next_line(&mut line).unwrap();
        assert!(line.contains("old"));

        // Same length, and the timestamp put back: nothing about the stat
        // says this file was touched.
        let rewritten = original.replace("old", "new");
        assert_eq!(rewritten.len(), original.len());
        fs::write(&path, &rewritten).unwrap();
        fs::File::open(&path)
            .unwrap()
            .set_modified(opened_at)
            .unwrap();
        let after = fs::metadata(&path).unwrap();
        assert_eq!(after.len() as usize, original.len());
        assert_eq!(after.modified().unwrap(), opened_at);

        assert!(
            matches!(
                reader.commit(original.len() as u64).unwrap(),
                CommitOutcome::Superseded
            ),
            "a rewrite with an unchanged stat must still supersede the pass"
        );

        // Positive control: a file nobody touched still publishes, so this is
        // the window being compared and not commits being refused.
        let mut quiet = TranscriptReader::open(&path, None, None).unwrap();
        quiet.next_line(&mut line).unwrap();
        let CommitOutcome::Published(file) = quiet.commit(rewritten.len() as u64).unwrap() else {
            panic!("an untouched file must still commit");
        };
        assert_eq!(file.offset, rewritten.len() as u64);
    }

    /// A rewritten sidecar is the parent hydration's news to report.
    ///
    /// `absorb` carried `superseded` from a pass into the outcome and
    /// `absorb_outcome` dropped it folding one transcript's outcome into the
    /// hydration's, so a sidecar that recorded no cursor — correctly — left
    /// the hydration containing it saying nothing about why.
    #[test]
    fn a_rewritten_sidecar_is_reported_by_the_parent_hydration() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-sidecar-rewritten";
        let transcript = seed_claude_transcript(
            dir.path(),
            session_id,
            format!(
                "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/work/app\",\
                 \"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"parent\"}},\
                 \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
            )
            .as_bytes(),
        );
        let sidecar = transcript.parent().unwrap().join("agent-child.jsonl");
        let child = format!(
            "{{\"sessionId\":\"{session_id}\",\"agentId\":\"child-1\",\"isSidechain\":true,\
             \"uuid\":\"s-1\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\
             \"message\":{{\"role\":\"assistant\",\"content\":\"old\"}},\
             \"timestamp\":\"2026-08-31T10:00:01Z\"}}\n"
        );
        let rewritten = child.replace("\"content\":\"old\"", "\"content\":\"new\"");
        assert_eq!(rewritten.len(), child.len());
        fs::write(&sidecar, child.as_bytes()).unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);

        // Only the sidecar's own commit disturbs the sidecar, so the pass that
        // parsed it is the pass that finds it moved. A hook that fired on the
        // parent's commit would rewrite the child before anything read it,
        // which is not this defect.
        let target = sidecar.clone();
        let bytes = rewritten.clone();
        let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let armed = std::sync::Arc::clone(&fired);
        super::cursor::set_before_commit_hook_for_test(Some(Box::new(move |committing| {
            if committing != target || armed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            fs::write(&target, &bytes).unwrap();
            bump_mtime(&target);
        })));
        let during =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        super::cursor::set_before_commit_hook_for_test(None);
        assert!(
            fired.load(std::sync::atomic::Ordering::SeqCst),
            "the sidecar must be rewritten between its read and its commit"
        );
        assert!(
            diagnostic(&during, "HYDRATION_SOURCE_REWRITTEN").is_some(),
            "a hydration reports a sidecar that recorded no cursor"
        );

        // Positive control: with nothing rewritten the hydration is quiet, so
        // the diagnostic tracks the event rather than the fixture.
        let quiet =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        assert!(diagnostic(&quiet, "HYDRATION_SOURCE_REWRITTEN").is_none());
    }

    /// Hydrate a transcript of `records` lines, append one more, and report
    /// what the second hydration read.
    fn append_cost_for_transcript(records: usize, session_id: &str) -> (i64, usize, usize) {
        let dir = tempfile::tempdir().unwrap();
        let mut body = String::new();
        for index in 0..records {
            body.push_str(&format!(
                "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-{index}\",\"cwd\":\"/w\",\
                 \"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"{}\"}},\
                 \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n",
                "x".repeat(200)
            ));
        }
        let transcript = seed_claude_transcript(dir.path(), session_id, body.as_bytes());
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_row(&conn, "claude", session_id, Some(&transcript));
        drop(conn);
        hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();

        let addition = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-appended\",\"cwd\":\"/w\",\
             \"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"appended\"}},\
             \"timestamp\":\"2026-08-31T10:01:00Z\"}}\n"
        );
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        write!(file, "{addition}").unwrap();
        drop(file);
        let appended =
            hydrate_session_at_with_home(&db, &options("claude", session_id), dir.path()).unwrap();
        (appended.bytes_read, body.len(), addition.len())
    }

    /// Validating a cursor reads provider bytes, and `bytesRead` is every
    /// provider byte a pass read.
    ///
    /// `open` hashes the saved cursor's window and the file's own; `commit`
    /// hashes the opened region again. None of it reached the counter, so a
    /// pass that hashed a megabyte reported only the records it parsed — the
    /// same omission this counter has been corrected for in every round, and
    /// it kept recurring because each read site added itself by hand. The
    /// hashing is counted where it is spent now, in `prefix_window_digest`.
    #[test]
    fn cursor_validation_reads_are_counted() {
        // Large enough that every validation window is full-sized, so what is
        // measured is the window rather than a short file.
        let (appended, body_len, addition) =
            append_cost_for_transcript(2_000, "session-validation-bytes");
        assert!(
            body_len as u64 > 2 * super::cursor::PREFIX_WINDOW_BYTES,
            "the fixture must be larger than the validation windows"
        );
        let digest = 2 * super::cursor::PREFIX_WINDOW_BYTES as i64;
        assert!(
            appended >= 2 * addition as i64 + 2 * digest,
            "bytes_read {appended} omits the validation windows the pass hashed \
             (append {addition}, one digest {digest})"
        );

        // Positive control, and the claim worth making: validation is bounded
        // by the *window*, not by the file, so the same append against a
        // transcript twice the size costs the same. A ceiling expressed as a
        // fraction of this file is what I reached for first, and it failed —
        // for a file this small the windows really do add up to more than the
        // file. That is the trade: constant work per pass, whatever the
        // transcript's size, which is what makes it sound on the 200 MB
        // transcripts this exists for.
        // The same session id, in its own temp directory: the appended record
        // has to be byte-identical for the comparison to mean anything.
        let (twice, twice_body, twice_addition) =
            append_cost_for_transcript(4_000, "session-validation-bytes");
        assert!(twice_body > body_len + body_len / 2);
        assert_eq!(twice_addition, addition);
        assert_eq!(
            twice, appended,
            "an append must cost the same on a transcript twice the size"
        );
    }

    /// Records read and validation paid for are reported as separate figures,
    /// so neither hides inside the other.
    ///
    /// Driven through one pass, where both are exact. `bytesRead` stays the
    /// total, which is what the contract documents.
    #[test]
    fn a_pass_separates_the_records_it_read_from_the_windows_it_hashed() {
        let dir = tempfile::tempdir().unwrap();
        let session_id = "session-validation-split";
        let transcript = dir.path().join("validation-split.jsonl");
        let first = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-1\",\"cwd\":\"/w\",\"type\":\"user\",\
             \"message\":{{\"role\":\"user\",\"content\":\"first\"}},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
        );
        fs::write(&transcript, first.as_bytes()).unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();

        let first_pass = crate::ingest::incremental::ingest_claude_transcript_at_locator(
            &conn,
            &transcript,
            None,
        )
        .unwrap();
        assert_eq!(
            first_pass.bytes_read - first_pass.validation_bytes,
            first.len() as u64,
            "the records are the file"
        );
        assert!(
            first_pass.validation_bytes > 0,
            "opening and committing a cursor hashes provider bytes, and they count"
        );

        let addition = format!(
            "{{\"sessionId\":\"{session_id}\",\"uuid\":\"u-2\",\"cwd\":\"/w\",\"type\":\"user\",\
             \"message\":{{\"role\":\"user\",\"content\":\"second\"}},\
             \"timestamp\":\"2026-08-31T10:00:01Z\"}}\n"
        );
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        write!(file, "{addition}").unwrap();
        drop(file);

        let appended = crate::ingest::incremental::ingest_claude_transcript_at_locator(
            &conn,
            &transcript,
            None,
        )
        .unwrap();
        // The record figure is exactly the append — the property this whole
        // change exists for — and the validation figure is what checking the
        // cursor cost, stated rather than folded in.
        assert_eq!(
            appended.bytes_read - appended.validation_bytes,
            addition.len() as u64,
            "an append costs its own size in records"
        );
        assert_eq!(
            appended.bytes_read,
            addition.len() as u64 + appended.validation_bytes,
            "and bytes_read is still the total of the two"
        );
        // Positive control: validation is bounded by the window, not by the
        // file, so it cannot be the file being re-read under another name.
        assert!(
            appended.validation_bytes <= 8 * super::cursor::PREFIX_WINDOW_BYTES,
            "validation is a fixed handful of bounded digests: {} bytes",
            appended.validation_bytes
        );
    }

    #[test]
    fn a_cursor_document_preserves_keys_it_does_not_understand() {
        // Sibling work parks its own per-source resume state in the same
        // document. A reader that does not know a key must hand it back
        // unchanged rather than drop it, or two changes silently erase each
        // other's state on alternate passes.
        let raw = serde_json::json!({
            "v": super::super::cursor::TRANSCRIPT_CURSOR_VERSION,
            "file": {"offset": 12, "mtime_ns": 7, "size": 12, "prefix_hash": "deadbeef"},
            "claude": {"next_line_index": 3},
            "tool_results": {"call_index": 9, "event_index": 41},
        })
        .to_string();
        let decoded = TranscriptCursorState::decode(Some(&raw));
        assert_eq!(decoded.committed_offset(), 12);
        assert_eq!(decoded.claude.as_ref().unwrap().next_line_index, 3);
        let round_tripped: Value = serde_json::from_str(&decoded.encode()).unwrap();
        assert_eq!(
            round_tripped["tool_results"],
            serde_json::json!({"call_index": 9, "event_index": 41}),
            "an unrecognized per-source key survives a round trip"
        );
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
        let env = DiscoveryEnv::with_roots(&conn, dir.path().into(), source.clone());
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
}
