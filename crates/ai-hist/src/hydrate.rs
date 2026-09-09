//! Targeted, provider-bounded session evidence acquisition.

use super::*;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;

pub const SESSION_HYDRATION_CONTRACT_VERSION: u32 = 2;
/// Bumped to 2 when Claude subagent transcripts that carry an `agentId`
/// started being indexed under that child id: existing databases re-parse once
/// and the earlier parent-attributed rows are healed in place.
const HYDRATION_PARSER_VERSION: i64 = 2;

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
    records: i64,
    path: Option<PathBuf>,
    /// Claude subagent sidecars, parsed once while stamping the source so the
    /// ingestion pass does not walk and re-parse the same files.
    claude_subagents: Vec<ClaudeSubagentEvidence>,
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
    hydrate_session_at_with_home(db_path, options, &home_dir())
}

fn hydrate_session_at_with_home(
    db_path: &Path,
    options: &HydrateSessionOptions,
    home: &Path,
) -> Result<HydrateSessionResult> {
    validate_options(options)?;
    let started = Instant::now();
    // Acquisition and replacement are one per-session critical section. The
    // provider call has to be inside it: otherwise an older response can wait
    // behind a newer writer and then replace that newer evidence. A file lock
    // covers both threads and independent RelayHistory processes.
    let _remote_lock = (options.scope == SessionScope::Remote)
        .then(|| acquire_remote_hydration_lock(db_path, options))
        .transpose()?;
    let mut conn = open_db(db_path)?;
    let target = catalog_target(&conn, options)?;
    if options.scope == SessionScope::Remote {
        return hydrate_remote_session(&mut conn, options, home, started);
    }
    let snapshot = source_snapshot(options, &target, home)?;
    let previous: Option<(Option<String>, i64, bool)> = conn
        .query_row(
            "SELECT source_stamp, parser_version, include_related FROM session_hydration_checkpoints \
             WHERE source = ? AND session_id = ? AND location = 'local'",
            params![options.source, options.session_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let previous_stamp = previous.as_ref().and_then(|(stamp, _, _)| stamp.clone());

    if previous_stamp.as_deref() == Some(snapshot.stamp.as_str())
        && previous
            .as_ref()
            .is_some_and(|(_, parser_version, _)| *parser_version == HYDRATION_PARSER_VERSION)
        && (!options.include_related || previous.as_ref().is_some_and(|(_, _, included)| *included))
        && target.discovery_state.as_deref() == Some("full")
    {
        return build_result(
            &conn,
            options,
            "unchanged",
            snapshot,
            started.elapsed().as_millis() as i64,
        );
    }

    // One selected provider session is one destination transaction. Provider
    // JSONL readers ignore an incomplete final record, and every evidence table
    // has a provider-native uniqueness key, so interruption followed by retry is
    // safe for both new and growing sessions.
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    ingest_selected(
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
            snapshot.records,
            options.include_related,
            now_ms(),
        ],
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
        started.elapsed().as_millis() as i64,
    )
}

struct RemoteHydrationLock {
    file: fs::File,
}

impl Drop for RemoteHydrationLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
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
    fs2::FileExt::lock_exclusive(&file)
        .with_context(|| format!("locking hydration target {}", lock_path.display()))?;
    Ok(RemoteHydrationLock { file })
}

fn hydrate_remote_session(
    conn: &mut Connection,
    options: &HydrateSessionOptions,
    home: &Path,
    started: Instant,
) -> Result<HydrateSessionResult> {
    if !matches!(options.source.as_str(), "claude" | "codex") {
        return remote_limited_result(
            conn,
            options,
            "CONNECTOR_NOT_CONFIGURED",
            format!(
                "no remote hydration connector exists for source '{}'",
                options.source
            ),
            started,
        );
    }
    let evidence =
        crate::remote::acquire_remote_session_at(home, &options.source, &options.session_id)
            .map_err(classify_remote_error)?;
    match evidence {
        crate::remote::RemoteSessionEvidence::CapabilityLimited { code, message } => {
            remote_limited_result(conn, options, code, message, started)
        }
        crate::remote::RemoteSessionEvidence::ClaudeFull {
            records,
            source_stamp,
            source_bytes,
        } => hydrate_remote_claude(conn, options, records, source_stamp, source_bytes, started),
        crate::remote::RemoteSessionEvidence::CodexDiff {
            diff,
            source_stamp,
            source_bytes,
        } => hydrate_remote_codex_diff(conn, options, &diff, source_stamp, source_bytes, started),
    }
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

fn hydrate_remote_claude(
    conn: &mut Connection,
    options: &HydrateSessionOptions,
    mut records: Vec<Value>,
    source_stamp: String,
    source_bytes: i64,
    started: Instant,
) -> Result<HydrateSessionResult> {
    let previous = hydration_checkpoint(conn, options, "remote")?;
    if previous.as_ref().is_some_and(|checkpoint| {
        checkpoint.source_stamp.as_deref() == Some(source_stamp.as_str())
            && checkpoint.parser_version == HYDRATION_PARSER_VERSION
    }) {
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
    let mut transcript = tempfile::NamedTempFile::new()?;
    for record in &records {
        serde_json::to_writer(&mut transcript, record)?;
        transcript.write_all(b"\n")?;
    }
    transcript.flush()?;
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
    if !had_local_presence {
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

fn hydrate_remote_codex_diff(
    conn: &mut Connection,
    options: &HydrateSessionOptions,
    diff: &str,
    source_stamp: String,
    source_bytes: i64,
    started: Instant,
) -> Result<HydrateSessionResult> {
    let previous = hydration_checkpoint(conn, options, "remote")?;
    let unchanged = previous.as_ref().is_some_and(|checkpoint| {
        checkpoint.source_stamp.as_deref() == Some(source_stamp.as_str())
            && checkpoint.parser_version == HYDRATION_PARSER_VERSION
    });
    if !unchanged {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM file_edits WHERE source = 'codex' AND session_id = ? AND tool_name = 'codex cloud diff'",
            [&options.session_id],
        )?;
        for (index, patch) in split_unified_diff(diff).into_iter().enumerate() {
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
                    format!("remote-diff:{index}"),
                    patch.path,
                    added,
                    removed,
                    serde_json::to_string(&serde_json::json!({"unified_diff": patch.text}))?,
                    now_ms(),
                ],
            )?;
        }
        write_hydration_checkpoint(&tx, options, "remote", &source_stamp, source_bytes, 1)?;
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

struct HydrationCheckpoint {
    source_stamp: Option<String>,
    parser_version: i64,
}

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
fn build_remote_result(
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
        presence: "remote".to_string(),
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
            "SELECT p.raw_locator, COALESCE(p.discovery_state, s.discovery_state) \
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
    let mut records = complete_jsonl_records(&path)?;
    let mut stamp = if options.source == "grok" {
        grok_session_stamp(&path)?
    } else {
        file_stamp(&path)?
    };
    let mut subagents = Vec::new();
    if options.source == "claude" && options.include_related {
        subagents = claude_subagents(&path, &options.session_id)?;
        for evidence in &subagents {
            stamp.push('|');
            stamp.push_str(&file_stamp(&evidence.path)?);
            bytes += evidence.path.metadata()?.len() as i64;
            records += complete_jsonl_records(&evidence.path)?;
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
    if options.source == "codex" && options.include_related {
        for child in codex_children(&path, &options.session_id)? {
            stamp.push('|');
            stamp.push_str(&file_stamp(&child)?);
            bytes += child.metadata()?.len() as i64;
            records += complete_jsonl_records(&child)?;
        }
    }
    Ok(SourceSnapshot {
        stamp,
        bytes,
        records,
        path: Some(path),
        claude_subagents: subagents,
    })
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

fn ingest_selected(
    conn: &Connection,
    options: &HydrateSessionOptions,
    target: &CatalogTarget,
    path: Option<&Path>,
    claude_subagents: &[ClaudeSubagentEvidence],
) -> Result<()> {
    match options.source.as_str() {
        "claude" => ingest_claude(conn, options, path.unwrap(), claude_subagents),
        "codex" => ingest_codex(conn, options, path.unwrap()),
        "cursor" => ingest_cursor(conn, options, target, path.unwrap()),
        "grok" => ingest_grok(conn, options, path.unwrap()),
        "opencode" => {
            sync_opencode_session(conn, path.unwrap(), &options.session_id)?;
            Ok(())
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
    record_claude_remote_relationship(conn, &meta)?;
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
        ingest_codex_children(conn, options, path)?;
    }
    Ok(())
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
    for candidate in collect_matching_files(directory, "rollout-", "jsonl")? {
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
        message: "targeted provider evidence acquisition completed".to_string(),
        duration_ms: Some(duration_ms),
        source_bytes: Some(snapshot.bytes),
        records_parsed: Some(snapshot.records),
    }];
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
                "UPDATE session_hydration_checkpoints SET parser_version = 0 WHERE source='claude' AND session_id='session-1'",
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
        catalog_row(&conn, "opencode", "selected", Some(&source));
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
        let result = hydrate_session_at_with_home(
            &db,
            &HydrateSessionOptions {
                source: "cursor".into(),
                session_id: "remote-cursor".into(),
                scope: SessionScope::Remote,
                include_related: true,
            },
            dir.path(),
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
