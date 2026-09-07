//! WS-9 cloud-sync increment 2b: the client transport for pushing the local recall store
//! to relayhistory-cloud (Agent Relay Loop).
//!
//! This is the **binding layer** — it does the network I/O the WASM-bound `ai-hist-core`
//! deliberately avoids. It wires `ai_hist_core::outbox::build_outbox_batch` (pure batch
//! building) to `POST /v1/ingest` with `rth_at_` bearer auth, persists a cursor per Cloud
//! stage, and advances only that stage's server-confirmed watermark.
//!
//! Token bootstrap: `/v1/cli/login` (RelayAuth JWT → `rth_at_`/`rth_rt_`) for real use, or
//! `/v1/admin/mint` (dev-only, `ADMIN_MINT_SECRET`) for local `wrangler dev` iteration.
//!
//! The HTTP call is behind the [`Ingestor`] trait so the push orchestration (batch build,
//! cursor advance, idempotent batchId, no-op-on-empty) is unit-testable without a server.

use ai_hist_core::convergence::{IngestRequest, IngestResponse, MachineIdentity};
use ai_hist_core::outbox::{build_outbox_batch, SyncCursor};
use ai_hist_core::turns::{build_turns_batch, ConversationTurn, DEFAULT_SESSION_BUDGET};
use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use url::Url;

const DEFAULT_BASE_URL: &str = "https://history.agentrelay.com";

/// Locally stored service-local session (never the RelayAuth JWT). Written `0600`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredAuth {
    /// Base URL of the relayhistory-cloud service, e.g. `http://localhost:8787`.
    pub base_url: String,
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    /// Local cache only — never authoritative; the server owns tenancy from the token.
    #[serde(default)]
    pub org_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
}

/// `~/.agentworkforce/relayhistory/` (override with `RELAYHISTORY_HOME` — used by tests).
pub fn config_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("RELAYHISTORY_HOME") {
        return PathBuf::from(dir);
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".agentworkforce/relayhistory")
}

pub fn default_base_url() -> String {
    ["RELAYHISTORY_BASE_URL", "AI_HIST_BASE_URL"]
        .into_iter()
        .filter_map(|key| std::env::var(key).ok())
        .find_map(|value| normalize_base_url(&value))
        .unwrap_or_else(|| DEFAULT_BASE_URL.to_string())
}

fn normalize_base_url(value: &str) -> Option<String> {
    let parsed = Url::parse(value.trim()).ok()?;
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return None;
    }
    Some(parsed.to_string().trim_end_matches('/').to_string())
}

fn normalized_stage(value: &str) -> Result<String> {
    normalize_base_url(value).with_context(|| {
        format!(
            "invalid relayhistory base URL `{}`; expected an absolute URL without credentials, query, or fragment",
            value.trim()
        )
    })
}

/// Refuse to put a credential on the wire in cleartext.
///
/// `base_url` is whatever `login`/`admin-mint` stored, and it is replayed with the `rth_at_`
/// bearer attached by every authenticated request below — `push` on a 300s timer, `pair
/// check` per invocation, `coverage` per run. A single `http://` in `auth.json` therefore
/// leaks the token to anything on the path, repeatedly.
///
/// Loopback is exempt, and deliberately without an opt-in flag: an `http://127.0.0.1`
/// request never reaches a network, so there is nothing to intercept, and `wrangler dev` on
/// `http://localhost:8787` is the documented local flow. Requiring an env var there would
/// buy no security and break the one case where plaintext is actually safe.
///
/// This is applied at every call site rather than only the newest one. Guarding `coverage`
/// alone would have left the high-frequency `push` path sending the same token to the same
/// URL — an inconsistent CLI that closes none of the exposure.
fn require_secure_transport(base_url: &str) -> Result<()> {
    let normalized = normalized_stage(base_url)?;
    let url = normalized.as_str();
    if url.starts_with("https://") {
        return Ok(());
    }
    if url
        .strip_prefix("http://")
        .is_some_and(authority_is_loopback)
    {
        return Ok(());
    }
    anyhow::bail!(
        "refusing to send the relayhistory bearer token in cleartext to `{url}` — use an \
         https:// endpoint. Plain http:// is accepted only for loopback (for example \
         http://127.0.0.1:8787, the `wrangler dev` address)."
    )
}

/// Does the authority of a URL — everything after `http://` — name this machine?
///
/// Parsed by hand rather than pulled in as a dependency, but the parts that matter for a
/// security decision are all handled: userinfo (`http://127.0.0.1@evil.com/`), the port, and
/// bracketed IPv6. A name that merely looks loopback-ish, like `127.0.0.1.evil.com`, fails
/// both the literal check and the IP parse, so it is rejected.
fn authority_is_loopback(rest: &str) -> bool {
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    // Userinfo is everything before the *last* `@`; the host is what follows it.
    let host_port = authority.rsplit('@').next().unwrap_or_default();
    let host = match host_port.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or_default(),
        None => host_port.split(':').next().unwrap_or_default(),
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn legacy_auth_path() -> PathBuf {
    config_dir().join("auth.json")
}

fn legacy_cursor_path() -> PathBuf {
    config_dir().join("cursor.json")
}

fn stage_key(base_url: &str) -> Result<String> {
    Ok(ai_hist_core::prompt_hash(&normalized_stage(base_url)?))
}

fn stage_dir() -> PathBuf {
    config_dir().join("stages")
}

fn auth_path(base_url: &str) -> Result<PathBuf> {
    Ok(stage_dir().join(format!("{}.auth.json", stage_key(base_url)?)))
}

fn cursor_path(base_url: &str) -> Result<PathBuf> {
    Ok(stage_dir().join(format!("{}.cursor.json", stage_key(base_url)?)))
}
fn machine_path() -> PathBuf {
    config_dir().join("machine-id")
}

fn write_private(path: &std::path::Path, body: &str) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    fs::create_dir_all(parent)?;

    static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);
    let file_name = path
        .file_name()
        .context("private state path has no file name")?
        .to_string_lossy();
    let tmp_path = parent.join(format!(
        ".{file_name}.tmp.{}.{}",
        std::process::id(),
        NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed)
    ));

    let saved = (|| -> Result<()> {
        let mut options = fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut tmp = options
            .open(&tmp_path)
            .with_context(|| format!("creating private state at {}", tmp_path.display()))?;
        tmp.write_all(body.as_bytes())?;
        tmp.sync_all()?;
        // `std::fs::rename` replaces an existing file on Unix, but not on
        // Windows. `replace_atomic` uses the platform replacement primitive so
        // every cursor/auth update has the same atomic overwrite semantics.
        atomicwrites::replace_atomic(&tmp_path, path)
            .with_context(|| format!("atomically replacing {}", path.display()))?;
        // A directory fsync makes the rename durable on filesystems that support it. Some
        // platforms reject directory sync, so it remains best effort after the file itself is
        // safely flushed and replaced.
        let _ = fs::File::open(parent).and_then(|dir| dir.sync_all());
        Ok(())
    })();
    if saved.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    saved
}

fn read_auth(path: &std::path::Path) -> Result<StoredAuth> {
    let body = fs::read_to_string(path)?;
    serde_json::from_str(&body).with_context(|| format!("parsing {}", path.display()))
}

fn staged_auths() -> Result<Vec<StoredAuth>> {
    let dir = stage_dir();
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut auths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".auth.json"))
        {
            auths.push(read_auth(&path)?);
        }
    }
    Ok(auths)
}

fn same_stage(left: &str, right: &str) -> bool {
    matches!(
        (normalize_base_url(left), normalize_base_url(right)),
        (Some(left), Some(right)) if left == right
    )
}

/// Load one stored session. A caller that knows its destination must pass it explicitly.
/// Without a destination, only a single configured stage is accepted; choosing the last
/// successful login was the old behavior and could silently divert a scheduled push.
pub fn load_auth(base_url: Option<&str>) -> Result<Option<StoredAuth>> {
    if let Some(base_url) = base_url {
        let path = auth_path(base_url)?;
        if path.exists() {
            let auth = read_auth(&path)?;
            anyhow::ensure!(
                same_stage(&auth.base_url, base_url),
                "stored session stage does not match the requested --base-url"
            );
            return Ok(Some(auth));
        }
        let legacy = legacy_auth_path();
        if !legacy.exists() {
            return Ok(None);
        }
        let auth = read_auth(&legacy)?;
        return Ok(same_stage(&auth.base_url, base_url).then_some(auth));
    }

    let mut auths = staged_auths()?;
    let legacy = legacy_auth_path();
    if legacy.exists() {
        let legacy_auth = read_auth(&legacy)?;
        if !auths
            .iter()
            .any(|auth| same_stage(&auth.base_url, &legacy_auth.base_url))
        {
            auths.push(legacy_auth);
        }
    }
    match auths.len() {
        0 => Ok(None),
        1 => Ok(auths.into_iter().next()),
        count => anyhow::bail!(
            "{count} relayhistory stages are configured; pass --base-url to select one. \
             Refusing to guess, because a global cloud session can skip records in another stage."
        ),
    }
}

/// Store a session only under its base URL. If upgrading from the old one-file layout, preserve
/// that old stage and cursor before adding the new one; overwriting it is exactly what caused
/// cross-stage cursor corruption.
pub fn save_auth(auth: &StoredAuth) -> Result<()> {
    migrate_legacy_stage()?;
    let mut stored = auth.clone();
    stored.base_url = normalized_stage(&auth.base_url)?;
    write_private(
        &auth_path(&stored.base_url)?,
        &serde_json::to_string_pretty(&stored)?,
    )
}

fn migrate_legacy_stage() -> Result<()> {
    let legacy_auth = legacy_auth_path();
    if !legacy_auth.exists() {
        return Ok(());
    }
    let mut auth = read_auth(&legacy_auth)?;
    auth.base_url = normalized_stage(&auth.base_url)?;
    let staged_auth_path = auth_path(&auth.base_url)?;
    if !staged_auth_path.exists() {
        write_private(&staged_auth_path, &serde_json::to_string_pretty(&auth)?)?;
    }

    let legacy_cursor = legacy_cursor_path();
    if legacy_cursor.exists() {
        let staged_cursor = cursor_path(&auth.base_url)?;
        if !staged_cursor.exists() {
            let body = fs::read_to_string(&legacy_cursor)?;
            write_private(&staged_cursor, &body)?;
        }
    }

    fs::remove_file(legacy_auth)?;
    if legacy_cursor.exists() {
        fs::remove_file(legacy_cursor)?;
    }
    Ok(())
}

pub fn load_cursor(base_url: &str) -> Result<SyncCursor> {
    let path = cursor_path(base_url)?;
    if path.exists() {
        let body = fs::read_to_string(&path)?;
        return serde_json::from_str(&body).context("parsing stage-scoped cursor.json");
    }

    // Compatibility for an existing single-stage install. The next successful push writes the
    // scoped path; a subsequent login migrates it eagerly with the corresponding auth session.
    let legacy_auth = legacy_auth_path();
    let legacy = legacy_cursor_path();
    if !legacy_auth.exists() || !legacy.exists() {
        return Ok(SyncCursor::default());
    }
    let auth = read_auth(&legacy_auth)?;
    if !same_stage(&auth.base_url, base_url) {
        return Ok(SyncCursor::default());
    }
    let body = fs::read_to_string(&legacy)?;
    serde_json::from_str(&body).context("parsing legacy cursor.json")
}

pub fn save_cursor(base_url: &str, cursor: &SyncCursor) -> Result<()> {
    let _cursor_lock = acquire_cursor_lock(base_url)?;
    let current = load_cursor(base_url)?;
    let merged = current.merge_max(cursor);
    write_private(
        &cursor_path(base_url)?,
        &serde_json::to_string_pretty(&merged)?,
    )
}

struct CursorWriteLock {
    file: fs::File,
}

impl Drop for CursorWriteLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

fn acquire_cursor_lock(base_url: &str) -> Result<CursorWriteLock> {
    let cursor = cursor_path(base_url)?;
    let mut name = cursor
        .file_name()
        .context("stage cursor path has no file name")?
        .to_os_string();
    name.push(".lock");
    let path = cursor.with_file_name(name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)?;
    fs2::FileExt::lock_exclusive(&file)
        .with_context(|| format!("locking cursor state at {}", path.display()))?;
    Ok(CursorWriteLock { file })
}

/// Stable per-machine id (the WS-1 `machineId` sub-tenant), generated once and persisted.
pub fn machine_id() -> Result<String> {
    let path = machine_path();
    if let Ok(existing) = fs::read_to_string(&path) {
        let trimmed = existing.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }
    let host = hostname();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let id = format!(
        "m_{}",
        ai_hist_core::prompt_hash(&format!("{host}:{nanos}"))
    );
    write_private(&path, &id)?;
    Ok(id)
}

/// This machine's name, or `None` if the OS will not tell us.
///
/// `$HOSTNAME` wins when set, so a test or a deliberately-labelled box can override it.
/// Otherwise ask the OS directly, which is the part that was missing: `HOSTNAME` is a shell
/// convenience, not part of the environment `launchd` (or `systemd`) hands a service. The
/// scheduled push therefore reported no hostname at all, and every pushing machine arrived
/// as an anonymous row — exactly the identity `coverage` exists to show. A manual
/// `ai-hist push` from a terminal looked fine, which is why it went unnoticed.
pub fn machine_hostname() -> Option<String> {
    fn clean(raw: &str) -> Option<String> {
        let trimmed = raw.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    }
    std::env::var("HOSTNAME")
        .ok()
        .and_then(|value| clean(&value))
        .or_else(|| {
            hostname::get()
                .ok()
                .and_then(|name| clean(&name.to_string_lossy()))
        })
}

/// The same name with a placeholder for the one caller that needs an infallible string.
fn hostname() -> String {
    machine_hostname().unwrap_or_else(|| "unknown-host".to_string())
}

/// Deterministic, retry-safe batch id: a hash of the batch's contents. Re-pushing the same
/// (machine, cursor span, record count) batch reuses the id, so the server's
/// `(orgId, machineId, batchId)` dedup makes a retry a no-op.
pub fn batch_id(machine: &str, from: &SyncCursor, to: &SyncCursor, count: usize) -> String {
    format!(
        "b_{}",
        ai_hist_core::prompt_hash(&format!(
            "{machine}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{count}",
            from.history_id,
            from.trajectory_rowid,
            from.trajectory_updated_ms,
            from.commit_link_id,
            from.file_edit_id,
            from.capture_version,
            to.history_id,
            to.trajectory_rowid,
            to.trajectory_updated_ms,
            to.commit_link_id,
            to.file_edit_id,
            to.capture_version
        ))
    )
}

/// Percent-encode one URL path segment.
///
/// Session ids come from harness files, not from us. A raw `/` or `?` in one would
/// silently retarget the request at a different endpoint — and the server would answer
/// that other request successfully, so nothing downstream would look wrong. Unreserved
/// characters (RFC 3986 §2.3) pass through; everything else is escaped.
fn encode_path_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// The HTTP side of `/v1/ingest`, abstracted so the push orchestration is testable.
pub trait Ingestor {
    fn ingest(&self, auth: &StoredAuth, req: &IngestRequest) -> Result<IngestResponse>;

    /// `POST /v1/sessions/:sessionId/turns` — one chunk of a session's transcript.
    ///
    /// Defaulted so existing test doubles keep compiling; the live ureq implementation
    /// overrides it. A double that does not override reports zero published turns, which
    /// is what "this test is not about turns" should look like.
    fn publish_turns(
        &self,
        _auth: &StoredAuth,
        _session_id: &str,
        _turns: &[ConversationTurn],
    ) -> Result<u64> {
        Ok(0)
    }
}

/// Result of a `push` run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushReport {
    pub sent: usize,
    pub accepted: u64,
    pub cursor: SyncCursor,
    pub batch_id: Option<String>,
    /// The per-source scan limit that produced the accepted batch.
    pub batch_limit: usize,
    /// Number of ingest attempts in this push run, including any smaller retry.
    pub attempts: usize,
    /// Conversation turns accepted by `/v1/sessions/:id/turns` in this run.
    pub turns_sent: u64,
    /// Sessions whose transcript was published in this run.
    pub turns_sessions: usize,
}

/// A typed Cloudflare Worker capacity rejection. Keeping the status, body, and attempted
/// record count together lets the retry loop distinguish a real resource-limit response from
/// an unrelated 503 and shrink from the payload that was actually emitted.
#[derive(Debug)]
struct WorkerCapacityError {
    status: u16,
    body: String,
    attempted_records: usize,
}

impl std::fmt::Display for WorkerCapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ingest failed: HTTP {}: {}",
            self.status,
            self.body.trim()
        )
    }
}

impl std::error::Error for WorkerCapacityError {}

/// Build the next outbox batch and push it. On success, persists the advanced cursor.
/// No-op (no HTTP call) when there's nothing new to send.
pub fn push(
    conn: &Connection,
    client: &dyn Ingestor,
    auth: &StoredAuth,
    machine: &MachineIdentity,
    cursor: &SyncCursor,
    limit: usize,
    incognito: &HashSet<String>,
) -> Result<PushReport> {
    let mut batch_limit = limit.max(1);
    let mut attempts = 1;

    loop {
        match push_once(
            conn,
            client,
            auth,
            machine,
            cursor,
            batch_limit,
            incognito,
            attempts,
        ) {
            Ok(report) => return Ok(report),
            Err(err) => {
                let Some(attempted_records) = worker_capacity_attempted_records(&err) else {
                    return Err(err);
                };
                if batch_limit <= 1 || attempted_records <= 1 {
                    return Err(err.context(format!(
                        "cloud ingest still exceeded its resource limit at the minimum batch \
                         limit after {attempts} attempt(s) ({attempted_records} emitted \
                         record(s)); cursor was not advanced"
                    )));
                }
                // The outbox applies this limit per source, and one trajectory row can emit
                // several records. Halving the emitted count can therefore reproduce the same
                // per-source limit and retry the identical rejected payload forever. Every
                // retry must strictly lower the controlling limit.
                batch_limit = (attempted_records / 2).min(batch_limit / 2).max(1);
                attempts += 1;
            }
        }
    }
}

/// Publish conversation transcripts for sessions with new events, after the convergence
/// batch has been accepted.
///
/// Ordering matters: turns are published only once `/v1/ingest` has succeeded, so a run
/// that fails on the convergence batch does not advance the transcript watermark either.
/// A failure here is logged and swallowed rather than failing the push — the convergence
/// records are already durably accepted at that point, and turning a transcript hiccup
/// into a push failure would roll the whole run back and re-send them.
fn publish_session_turns(
    conn: &Connection,
    client: &dyn Ingestor,
    auth: &StoredAuth,
    cursor: &SyncCursor,
    incognito: &HashSet<String>,
) -> (u64, usize, i64) {
    let batch = match build_turns_batch(
        conn,
        cursor.session_event_id,
        DEFAULT_SESSION_BUDGET,
        incognito,
    ) {
        Ok(batch) => batch,
        Err(_) => return (0, 0, cursor.session_event_id),
    };
    if batch.sessions.is_empty() {
        return (0, 0, batch.session_event_id);
    }

    let mut accepted = 0u64;
    let mut sessions = 0usize;
    for session in &batch.sessions {
        let mut whole_session_ok = true;
        for chunk in &session.chunks {
            match client.publish_turns(auth, &session.session_id, chunk) {
                Ok(count) => accepted += count,
                Err(_) => {
                    whole_session_ok = false;
                    break;
                }
            }
        }
        if !whole_session_ok {
            // Hold the watermark at the last fully published session. Advancing past a
            // half-published transcript would leave it permanently truncated, and the
            // next run would report a clean `turns_sent: 0`.
            return (accepted, sessions, cursor.session_event_id);
        }
        sessions += 1;
    }

    (accepted, sessions, batch.session_event_id)
}

fn push_once(
    conn: &Connection,
    client: &dyn Ingestor,
    auth: &StoredAuth,
    machine: &MachineIdentity,
    cursor: &SyncCursor,
    batch_limit: usize,
    incognito: &HashSet<String>,
    attempts: usize,
) -> Result<PushReport> {
    let batch = build_outbox_batch(conn, cursor, batch_limit, incognito)?;
    if batch.records.is_empty() {
        // Transcripts are published here too, not only on the path with convergence
        // records. The two streams have independent watermarks, and a machine whose
        // prompts are fully synced still has a transcript backlog — gating turns on new
        // prompts would strand it behind a permanent `sent: 0`.
        let mut advanced = batch.cursor;
        let (turns_sent, turns_sessions, session_event_id) =
            publish_session_turns(conn, client, auth, &advanced, incognito);
        advanced.session_event_id = session_event_id;

        // Incognito and zero-emission rows still advance scan cursors. Persist that progress
        // even though there is no payload, otherwise a reduced capacity retry can report a
        // no-op and rebuild the original oversized request on the next run.
        if advanced != *cursor {
            save_cursor(&auth.base_url, &advanced)?;
        }
        return Ok(PushReport {
            sent: 0,
            accepted: 0,
            cursor: advanced,
            batch_id: None,
            batch_limit,
            // `attempts` names the next attempted HTTP call. If this empty batch followed
            // rejected requests, retain the number of calls already made; a first-run no-op
            // still reports zero.
            attempts: attempts.saturating_sub(1),
            turns_sent,
            turns_sessions,
        });
    }
    let bid = batch_id(&machine.id, cursor, &batch.cursor, batch.records.len());
    let req = IngestRequest {
        machine: machine.clone(),
        batch_id: bid.clone(),
        cursors: Some(serde_json::json!({
            "history_id": batch.cursor.history_id,
            "trajectory_rowid": batch.cursor.trajectory_rowid,
            "trajectory_updated_ms": batch.cursor.trajectory_updated_ms,
            "commit_link_id": batch.cursor.commit_link_id,
            "file_edit_id": batch.cursor.file_edit_id,
            "capture_version": batch.cursor.capture_version,
        })),
        records: batch.records,
    };
    let resp = client.ingest(auth, &req).context("POST /v1/ingest")?;
    // advance the cursor only after the server accepts the batch (durable outbox)
    let mut advanced = batch.cursor;
    let (turns_sent, turns_sessions, session_event_id) =
        publish_session_turns(conn, client, auth, &advanced, incognito);
    advanced.session_event_id = session_event_id;
    save_cursor(&auth.base_url, &advanced)?;
    Ok(PushReport {
        sent: req.records.len(),
        accepted: resp.accepted,
        cursor: advanced,
        batch_id: Some(bid),
        batch_limit,
        attempts,
        turns_sent,
        turns_sessions,
    })
}

fn worker_capacity_attempted_records(err: &anyhow::Error) -> Option<usize> {
    err.chain().find_map(|cause| {
        cause
            .downcast_ref::<WorkerCapacityError>()
            .map(|failure| failure.attempted_records)
    })
}

fn ingest_status_error(code: u16, body: String, attempted_records: usize) -> anyhow::Error {
    let normalized = body.to_ascii_lowercase();
    let is_capacity = code == 503
        && (normalized.contains("worker exceeded resource limits")
            || normalized.contains("error code: 1102")
            || normalized.contains("worker_capacity"));
    if is_capacity {
        WorkerCapacityError {
            status: code,
            body,
            attempted_records,
        }
        .into()
    } else {
        anyhow::anyhow!("ingest failed: HTTP {code}: {body}")
    }
}

fn map_ingest_http_err(error: ureq::Error, attempted_records: usize) -> anyhow::Error {
    match error {
        ureq::Error::Status(code, response) => ingest_status_error(
            code,
            response.into_string().unwrap_or_default(),
            attempted_records,
        ),
        other => other.into(),
    }
}

// ----- ureq-backed live transport -----

/// Live `Ingestor` over `ureq` (blocking HTTP — no async runtime, never compiled into the
/// WASM core).
pub struct UreqIngestor;

impl Ingestor for UreqIngestor {
    fn ingest(&self, auth: &StoredAuth, req: &IngestRequest) -> Result<IngestResponse> {
        require_secure_transport(&auth.base_url)?;
        let url = format!("{}/v1/ingest", auth.base_url.trim_end_matches('/'));
        let payload = serde_json::to_value(req)?;
        let response = send_with_auth_refresh(
            auth,
            |current| {
                ureq::post(&url)
                    .set("Authorization", &format!("Bearer {}", current.access_token))
                    .set("Content-Type", "application/json")
                    .send_json(payload.clone())
                    .map_err(Box::new)
            },
            |error| map_ingest_http_err(error, req.records.len()),
        )
        .context("ingest failed")?;
        Ok(response.into_json::<IngestResponse>()?)
    }

    fn publish_turns(
        &self,
        auth: &StoredAuth,
        session_id: &str,
        turns: &[ConversationTurn],
    ) -> Result<u64> {
        if turns.is_empty() {
            return Ok(0);
        }
        require_secure_transport(&auth.base_url)?;
        // The session id is a path segment and can contain characters that are not URL
        // safe, so encode it rather than interpolating it raw.
        let url = format!(
            "{}/v1/sessions/{}/turns",
            auth.base_url.trim_end_matches('/'),
            encode_path_segment(session_id),
        );
        let payload = serde_json::json!({ "turns": turns });
        let response = send_with_auth_refresh(
            auth,
            |current| {
                ureq::post(&url)
                    .set("Authorization", &format!("Bearer {}", current.access_token))
                    .set("Content-Type", "application/json")
                    .send_json(payload.clone())
                    .map_err(Box::new)
            },
            |error| match error {
                ureq::Error::Status(code, response) => anyhow::anyhow!(
                    "turns failed: HTTP {code}: {}",
                    response.into_string().unwrap_or_default()
                ),
                other => other.into(),
            },
        )
        .context("publish turns failed")?;

        #[derive(serde::Deserialize)]
        struct TurnsResponse {
            #[serde(default)]
            accepted: u64,
        }
        Ok(response.into_json::<TurnsResponse>()?.accepted)
    }
}

struct AuthRefreshLock {
    _file: fs::File,
}

fn refresh_lock_path(base_url: &str) -> Result<PathBuf> {
    let auth = auth_path(base_url)?;
    let mut name = auth
        .file_name()
        .context("stage auth path has no file name")?
        .to_os_string();
    name.push(".refresh.lock");
    Ok(auth.with_file_name(name))
}

fn acquire_refresh_lock(base_url: &str) -> Result<AuthRefreshLock> {
    let path = refresh_lock_path(base_url)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)?;
    fs2::FileExt::lock_exclusive(&file)
        .with_context(|| format!("locking token refresh state at {}", path.display()))?;
    Ok(AuthRefreshLock { _file: file })
}

fn is_unauthorized(error: &ureq::Error) -> bool {
    matches!(error, ureq::Error::Status(401, _))
}

/// Send one authenticated request and rotate an expired session at most once. The persisted
/// session is loaded before each request so a long-lived caller holding an older `StoredAuth`
/// transparently adopts a pair rotated by an earlier call. Refresh-token rotation is serialized
/// per stage: a waiter reloads credentials after acquiring the lock and reuses a pair already
/// rotated by another process instead of submitting the revoked old token.
fn send_with_auth_refresh(
    auth: &StoredAuth,
    send: impl Fn(&StoredAuth) -> std::result::Result<ureq::Response, Box<ureq::Error>>,
    map_error: impl Fn(ureq::Error) -> anyhow::Error,
) -> Result<ureq::Response> {
    let attempted = load_auth(Some(&auth.base_url))?.unwrap_or_else(|| auth.clone());
    let mut unauthorized = match send(&attempted) {
        Ok(response) => return Ok(response),
        Err(error) if is_unauthorized(&error) => *error,
        Err(error) => return Err(map_error(*error)),
    };

    let _refresh_lock = acquire_refresh_lock(&auth.base_url)?;
    let current = load_auth(Some(&auth.base_url))?.unwrap_or_else(|| auth.clone());

    // A concurrent process may have completed rotation while this caller waited. Try its
    // persisted access token before touching the one-time refresh token again.
    if current.access_token != attempted.access_token {
        match send(&current) {
            Ok(response) => return Ok(response),
            Err(error) if is_unauthorized(&error) => unauthorized = *error,
            Err(error) => return Err(map_error(*error)),
        }
    }

    if current
        .refresh_token
        .as_deref()
        .is_none_or(|token| token.trim().is_empty())
    {
        return Err(map_error(unauthorized));
    }
    let refreshed = refresh_auth(&current).context("refreshing relayhistory session")?;
    // Refresh tokens rotate and invalidate their predecessor. Persist the new pair atomically
    // before retrying so a crash or network failure cannot strand the client on the old token.
    save_auth(&refreshed).context("persisting refreshed relayhistory session")?;
    send(&refreshed).map_err(|error| map_error(*error))
}

fn refresh_auth(auth: &StoredAuth) -> Result<StoredAuth> {
    require_secure_transport(&auth.base_url)?;
    let refresh_token = auth
        .refresh_token
        .as_deref()
        .context("stored relayhistory session has no refresh token; run `ai-hist login`")?;
    let url = format!(
        "{}/v1/auth/token/refresh",
        auth.base_url.trim_end_matches('/')
    );
    let response = ureq::post(&url)
        .set("Content-Type", "application/json")
        .send_json(serde_json::json!({ "refreshToken": refresh_token }))
        .map_err(map_http_err)?;
    let payload: serde_json::Value = response.into_json()?;
    Ok(StoredAuth {
        base_url: auth.base_url.clone(),
        access_token: field(&payload, "accessToken")?,
        refresh_token: Some(field(&payload, "refreshToken")?),
        org_id: auth.org_id.clone(),
        workspace_id: auth.workspace_id.clone(),
    })
}

/// `POST /v1/admin/mint` (dev-only bootstrap) → store the `rth_at_` session.
pub fn admin_mint(
    base_url: &str,
    admin_secret: &str,
    org_id: &str,
    workspace_id: Option<&str>,
    user_id: &str,
    label: &str,
) -> Result<StoredAuth> {
    require_secure_transport(base_url)?;
    let url = format!("{}/v1/admin/mint", base_url.trim_end_matches('/'));
    let mut body = serde_json::json!({ "orgId": org_id, "userId": user_id, "label": label });
    if let Some(ws) = workspace_id {
        body["workspaceId"] = serde_json::json!(ws);
    }
    let resp = ureq::post(&url)
        .set("x-admin-secret", admin_secret)
        .set("Content-Type", "application/json")
        .send_json(body)
        .map_err(map_http_err)?;
    let v: serde_json::Value = resp.into_json()?;
    Ok(StoredAuth {
        base_url: base_url.trim_end_matches('/').to_string(),
        access_token: field(&v, "accessToken")?,
        refresh_token: v
            .get("refreshToken")
            .and_then(|x| x.as_str())
            .map(String::from),
        org_id: Some(org_id.to_string()),
        workspace_id: workspace_id.map(String::from),
    })
}

/// `POST /v1/cli/login` (RelayAuth JWT → `rth_at_`/`rth_rt_`) — the real-use bootstrap.
pub fn login(
    base_url: &str,
    agent_relay_token: &str,
    label: &str,
    mode: Option<&str>,
) -> Result<StoredAuth> {
    require_secure_transport(base_url)?;
    let url = format!("{}/v1/cli/login", base_url.trim_end_matches('/'));
    let mut body = serde_json::json!({ "agentRelayToken": agent_relay_token, "label": label });
    if let Some(mode) = mode {
        body["mode"] = serde_json::json!(mode);
    }
    let resp = ureq::post(&url)
        .set("Content-Type", "application/json")
        .send_json(body)
        .map_err(map_http_err)?;
    let v: serde_json::Value = resp.into_json()?;
    Ok(StoredAuth {
        base_url: base_url.trim_end_matches('/').to_string(),
        access_token: field(&v, "accessToken")?,
        refresh_token: v
            .get("refreshToken")
            .and_then(|x| x.as_str())
            .map(String::from),
        org_id: v.get("orgId").and_then(|x| x.as_str()).map(String::from),
        workspace_id: v
            .get("workspaceId")
            .and_then(|x| x.as_str())
            .map(String::from),
    })
}

/// The canonical Agent Relay Cloud session returned by `agent-relay cloud session --json`.
/// This is the same credential source used by relayfile/workforce. It is captured only long
/// enough to exchange it for a service-local relayhistory session.
#[derive(Debug, serde::Deserialize)]
struct AgentRelayCloudSession {
    #[serde(rename = "apiUrl")]
    _api_url: Option<String>,
    #[serde(rename = "accessToken")]
    access_token: String,
}

fn env_agent_relay_session() -> Option<AgentRelayCloudSession> {
    std::env::var("CLOUD_API_ACCESS_TOKEN")
        .ok()
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
        .map(|access_token| AgentRelayCloudSession {
            _api_url: std::env::var("CLOUD_API_URL").ok(),
            access_token,
        })
}

fn agent_relay_bin() -> String {
    std::env::var("AGENT_RELAY_BIN").unwrap_or_else(|_| "agent-relay".to_string())
}

fn read_agent_relay_session(bin: &str) -> Result<AgentRelayCloudSession> {
    // `--reveal-token` is REQUIRED. Without it `agent-relay cloud session --json` returns the
    // access token *masked* (e.g. `cld_at_…Knv4`), which is not a usable bearer: relayhistory
    // forwards it to Cloud `api/v1/auth/whoami`, which rejects it, and `ai-hist login` fails with
    // `HTTP 401: {"error":"invalid Agent Relay Cloud token"}`. The masked form is still a
    // syntactically plausible `cld_at_…` string, so nothing upstream catches it.
    let output = std::process::Command::new(bin)
        .args(["cloud", "session", "--json", "--reveal-token"])
        .output()
        .with_context(|| {
            format!(
                "failed to run `{bin} cloud session --json --reveal-token` — install Agent Relay and run `agent-relay login`"
            )
        })?;
    if !output.status.success() {
        // Surface stderr (user-facing auth prompts/errors) but NEVER stdout — stdout contains the
        // bearer token when this command succeeds or partially succeeds.
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "Agent Relay Cloud session lookup failed ({}). {}",
            output.status,
            stderr.trim()
        );
    }
    serde_json::from_slice(&output.stdout).context(
        "parsing Agent Relay Cloud session JSON from `agent-relay cloud session --json --reveal-token`",
    )
}

fn ensure_agent_relay_session(
    bin: &str,
    workspace: Option<&str>,
) -> Result<AgentRelayCloudSession> {
    if let Some(ws) = workspace {
        anyhow::bail!(
            "`--workspace {ws}` is not supported for Cloud login yet because `agent-relay cloud session` has no non-mutating workspace-scoped mode; switch the active workspace with Agent Relay first, then rerun without `--workspace`"
        );
    }

    if let Some(session) = env_agent_relay_session() {
        return Ok(session);
    }

    match read_agent_relay_session(bin) {
        Ok(session) => Ok(session),
        Err(first_error) => {
            eprintln!("Agent Relay Cloud login required; starting `agent-relay cloud login`.");
            let status = std::process::Command::new(bin)
                .args(["cloud", "login"])
                .status()
                .with_context(|| {
                    format!(
                        "failed to run `{bin} cloud login` — install Agent Relay and run `agent-relay login`"
                    )
                })?;
            if !status.success() {
                anyhow::bail!(
                    "Agent Relay Cloud login failed ({status}). Previous session lookup error: {first_error}"
                );
            }
            read_agent_relay_session(bin)
        }
    }
}

/// Cloud login: use the canonical Agent Relay Cloud session, then exchange that bearer for a
/// service-local `rth_*` relayhistory session via `/v1/cli/login`.
pub fn login_via_cloud(
    base_url: &str,
    mode: &str,
    workspace: Option<&str>,
    label: &str,
) -> Result<StoredAuth> {
    if mode != "read" && mode != "sync" {
        anyhow::bail!("invalid --mode '{mode}' (expected `read` or `sync`)");
    }
    validate_cloud_exchange_base_url(base_url)?;
    let bin = agent_relay_bin();
    let session = ensure_agent_relay_session(&bin, workspace)?;
    login(base_url, &session.access_token, label, Some(mode))
}

fn validate_cloud_exchange_base_url(base_url: &str) -> Result<()> {
    let normalized = normalize_base_url(base_url).context("relayhistory base URL is empty")?;
    if normalized == DEFAULT_BASE_URL {
        return Ok(());
    }
    let allowed = std::env::var("RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    if allowed {
        return Ok(());
    }
    anyhow::bail!(
        "refusing to send the Agent Relay Cloud bearer to non-default relayhistory URL `{normalized}`; use manual `--token` login or set RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL=1 for a trusted dev endpoint"
    )
}

fn field(v: &serde_json::Value, key: &str) -> Result<String> {
    v.get(key)
        .and_then(|x| x.as_str())
        .map(String::from)
        .with_context(|| format!("response missing `{key}`"))
}

fn map_http_err(e: ureq::Error) -> anyhow::Error {
    match e {
        ureq::Error::Status(code, r) => {
            let body = r.into_string().unwrap_or_default();
            anyhow::anyhow!("HTTP {code}: {body}")
        }
        other => other.into(),
    }
}

/// Fetch every page in the server's ascending `(ts, eventId)` order.
pub fn replay_events(
    auth: &StoredAuth,
    session_id: &str,
    limit: Option<usize>,
    max_content: Option<usize>,
) -> Result<Vec<serde_json::Value>> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct EventPage {
        // Keep the event payload intact so --json preserves fields added by the server.
        events: Vec<serde_json::Value>,
        next_cursor: Option<String>,
    }

    require_secure_transport(&auth.base_url)?;
    let url = format!(
        "{}/v1/sessions/{}/events",
        normalized_stage(&auth.base_url)?,
        encode_path_segment(session_id)
    );
    let mut events = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    loop {
        let response = send_with_auth_refresh(
            auth,
            |current| {
                let mut request = ureq::get(&url)
                    .set("Authorization", &format!("Bearer {}", current.access_token))
                    .query("order", "asc");
                if let Some(value) = limit {
                    request = request.query("limit", &value.to_string());
                }
                if let Some(value) = max_content {
                    request = request.query("maxContent", &value.to_string());
                }
                if let Some(value) = &cursor {
                    request = request.query("cursor", value);
                }
                request.call().map_err(Box::new)
            },
            |error| match error {
                ureq::Error::Status(401, _) => anyhow::anyhow!(
                    "HTTP 401: relayhistory session is expired or invalid; run `ai-hist login` for the selected --base-url"
                ),
                other => map_http_err(other),
            },
        )
        .context("replay query failed (authentication failures require `ai-hist login` for the selected --base-url)")?;
        let page: EventPage = response.into_json().context("parsing replay events page")?;
        events.extend(page.events);
        match page.next_cursor {
            None => return Ok(events),
            Some(next) => {
                // Preserve the opaque cursor, including its event-id tiebreak. A short
                // page is not exhaustion, and a repeated cursor must fail instead of
                // looping forever or saving duplicated events as a complete transcript.
                anyhow::ensure!(
                    !next.is_empty() && seen_cursors.insert(next.clone()),
                    "replay returned an empty or repeated nextCursor; transcript is incomplete"
                );
                cursor = Some(next);
            }
        }
    }
}

// ----- WS-6 Pair: in-session warning check (client of POST /v1/pair/check) -----

/// Minimal current-session context sent to `/v1/pair/check`. **Never** file contents or
/// full prompt bodies — only paths + caller-provided summaries (egress-minimal).
#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct PairContext {
    #[serde(rename = "projectId", skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(rename = "repoPath", skip_serializing_if = "Option::is_none")]
    pub repo_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(rename = "gitRemote", skip_serializing_if = "Option::is_none")]
    pub git_remote: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(rename = "recentPrompt", skip_serializing_if = "Option::is_none")]
    pub recent_prompt: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct PairRequest<'a> {
    context: &'a PairContext,
    limit: usize,
}

/// One cited convergence event backing a warning (full composite identity; eventId isn't
/// unique alone). `snippet` is server-scrubbed + length-capped — never the raw `record`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PairEvidence {
    #[serde(rename = "machineId", default)]
    pub machine_id: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(rename = "sessionId", default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(rename = "eventId", default)]
    pub event_id: Option<String>,
    #[serde(default)]
    pub ts: Option<String>,
    #[serde(default)]
    pub snippet: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PairWarning {
    pub text: String,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub lens: Option<String>,
    #[serde(default)]
    pub score: Option<f64>,
    #[serde(default)]
    pub evidence: Vec<PairEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PairResponse {
    /// `"warn"` iff warnings present; `"allow"` otherwise. v1 is advisory — never blocks.
    #[serde(default)]
    pub decision: Option<String>,
    #[serde(default)]
    pub warnings: Vec<PairWarning>,
    #[serde(rename = "correlationId", default)]
    pub correlation_id: Option<String>,
}

/// `POST /v1/pair/check` with the stored `rth_at_` bearer. Returns ranked advisory warnings.
pub fn pair_check(auth: &StoredAuth, context: &PairContext, limit: usize) -> Result<PairResponse> {
    require_secure_transport(&auth.base_url)?;
    let url = format!("{}/v1/pair/check", auth.base_url.trim_end_matches('/'));
    let req = PairRequest { context, limit };
    let payload = serde_json::to_value(req)?;
    let response = send_with_auth_refresh(
        auth,
        |current| {
            ureq::post(&url)
                .set("Authorization", &format!("Bearer {}", current.access_token))
                .set("Content-Type", "application/json")
                .send_json(payload.clone())
                .map_err(Box::new)
        },
        map_http_err,
    )
    .context("pair check failed")?;
    Ok(response.into_json::<PairResponse>()?)
}

/// Human-readable rendering of Pair warnings (the non-`--json` output).
pub fn format_pair_warnings(resp: &PairResponse) -> String {
    if resp.warnings.is_empty() {
        return "No Pair warnings for this context.".to_string();
    }
    let mut out = format!("⚠️  {} Pair warning(s) [advisory]:\n", resp.warnings.len());
    for (i, w) in resp.warnings.iter().enumerate() {
        let kind = w.kind.as_deref().unwrap_or("?");
        let score = w.score.map(|s| format!(" ({s:.2})")).unwrap_or_default();
        out.push_str(&format!("{}. [{kind}{score}] {}\n", i + 1, w.text));
        if let Some(ev) = w.evidence.first() {
            let id = ev.event_id.as_deref().unwrap_or("?");
            out.push_str(&format!("   ↳ {id}\n"));
        }
    }
    out
}

// ----- Fleet coverage: client of GET /v1/machines -----

/// Push activity for one machine inside the server's reporting window.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CoverageRecent {
    #[serde(default)]
    pub batches: u64,
    #[serde(default)]
    pub records: u64,
    #[serde(default)]
    pub accepted: u64,
    #[serde(rename = "lastBatchAt", default)]
    pub last_batch_at: Option<String>,
    /// Records in the newest batch. Carried for `--json` consumers; deliberately not used to
    /// infer a backlog — see the note in `format_fleet_coverage`.
    #[serde(rename = "lastBatchRecordCount", default)]
    pub last_batch_record_count: Option<u64>,
}

/// One machine's coverage row. `status` is server-derived: `active`, `stale`, or `missing`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MachineCoverage {
    #[serde(rename = "machineId", default)]
    pub machine_id: String,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub os: Option<String>,
    #[serde(rename = "relayhistoryVersion", default)]
    pub relayhistory_version: Option<String>,
    #[serde(rename = "lastSeenAt", default)]
    pub last_seen_at: Option<String>,
    #[serde(rename = "secondsSinceLastSeen", default)]
    pub seconds_since_last_seen: u64,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub cursors: serde_json::Value,
    #[serde(default)]
    pub recent: CoverageRecent,
}

impl MachineCoverage {
    /// What to call this machine in output: hostname, else label, else the opaque id.
    pub fn display_name(&self) -> &str {
        self.hostname
            .as_deref()
            .filter(|s| !s.is_empty())
            .or(self.label.as_deref().filter(|s| !s.is_empty()))
            .unwrap_or(&self.machine_id)
    }

    pub fn is_reporting(&self) -> bool {
        self.status == "active"
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CoverageSummary {
    #[serde(default)]
    pub total: u64,
    #[serde(default)]
    pub active: u64,
    #[serde(default)]
    pub stale: u64,
    #[serde(default)]
    pub missing: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CoverageThresholds {
    #[serde(rename = "staleAfterSeconds", default)]
    pub stale_after_seconds: u64,
    #[serde(rename = "missingAfterSeconds", default)]
    pub missing_after_seconds: u64,
    #[serde(rename = "windowHours", default)]
    pub window_hours: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CoverageResponse {
    #[serde(default)]
    pub machines: Vec<MachineCoverage>,
    #[serde(default)]
    pub summary: CoverageSummary,
    #[serde(default)]
    pub thresholds: CoverageThresholds,
}

impl CoverageResponse {
    /// True when any machine has fallen behind — the condition `--fail-on-stale` reports.
    pub fn has_gaps(&self) -> bool {
        self.machines.iter().any(|m| !m.is_reporting())
    }
}

/// Knobs forwarded to the server so the caller can match its own push interval.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CoverageQuery {
    pub stale_after_seconds: Option<u64>,
    pub missing_after_seconds: Option<u64>,
    pub window_hours: Option<u64>,
}

/// `GET /v1/machines` with the stored `rth_at_` bearer.
pub fn fleet_coverage(auth: &StoredAuth, query: &CoverageQuery) -> Result<CoverageResponse> {
    require_secure_transport(&auth.base_url)?;
    let url = format!("{}/v1/machines", auth.base_url.trim_end_matches('/'));
    let response = send_with_auth_refresh(
        auth,
        |current| {
            let mut request =
                ureq::get(&url).set("Authorization", &format!("Bearer {}", current.access_token));
            if let Some(v) = query.stale_after_seconds {
                request = request.query("staleAfterSeconds", &v.to_string());
            }
            if let Some(v) = query.missing_after_seconds {
                request = request.query("missingAfterSeconds", &v.to_string());
            }
            if let Some(v) = query.window_hours {
                request = request.query("windowHours", &v.to_string());
            }
            request.call().map_err(Box::new)
        },
        map_http_err,
    )
    .context("coverage query failed")?;
    Ok(response.into_json::<CoverageResponse>()?)
}

/// Human-readable fleet table (the non-`--json` output).
///
/// The point of this rendering is that a mute machine cannot be skimmed past: it keeps its
/// row, gets a loud status, and is called out again underneath the table.
pub fn format_fleet_coverage(resp: &CoverageResponse) -> String {
    if resp.machines.is_empty() {
        return "No machines have pushed history to this org yet.\n".to_string();
    }

    let rows: Vec<[String; 6]> = resp
        .machines
        .iter()
        .map(|m| {
            [
                safe_cell(m.display_name()),
                status_cell(&m.status),
                format!("{} ago", humanize_secs(m.seconds_since_last_seen)),
                m.recent.batches.to_string(),
                m.recent.records.to_string(),
                // Already sanitized per entry. CURSOR is the last column and `render_row`
                // never pads the last cell, so its length cannot misalign another row.
                format_cursors(&m.cursors),
            ]
        })
        .collect();

    let headers = [
        "MACHINE".to_string(),
        "STATUS".to_string(),
        "LAST PUSH".to_string(),
        format!("{}H PUSHES", resp.thresholds.window_hours),
        "RECORDS".to_string(),
        "CURSOR".to_string(),
    ];
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }

    let s = &resp.summary;
    let mut out = format!(
        "Fleet coverage — {} machine(s): {} active, {} stale, {} missing\n\n",
        s.total, s.active, s.stale, s.missing
    );
    // Counts read as counts only when their digits line up.
    const RIGHT_ALIGNED: [bool; 6] = [false, false, false, true, true, false];
    out.push_str(&render_row(&headers, &widths, &RIGHT_ALIGNED));
    for row in &rows {
        out.push_str(&render_row(row, &widths, &RIGHT_ALIGNED));
    }

    // Repeat every gap in prose. A status column is easy to skim past; this is the line
    // that would have caught finn-mini.
    //
    // Only gaps get a note. Earlier revisions also called out a machine whose newest batch
    // reached 500 records, reading that as a backlog still draining. That inference does not
    // hold from anything in this payload:
    //
    // - 500 is `push --limit`'s *default*, not a fact about the fleet. A machine pushing
    //   with a smaller limit fills every batch and would be flagged forever; a machine with
    //   exactly 500 new records fills one batch and is already caught up.
    // - The server sends no has-more signal, so "the batch was full" is the whole of what is
    //   known — and it separates neither backlog from caught-up nor healthy from failing.
    //   sf-mini cannot complete a 500-record batch at all (it 503s above ~200,
    //   AgentWorkforce/relayhistory#58), so the one machine with a real ingest problem is
    //   precisely the one this note could never fire for.
    //
    // The batch and record columns carry the same evidence without asserting a cause.
    let mut notes = Vec::new();
    for m in &resp.machines {
        if !m.is_reporting() {
            notes.push(format!(
                "⚠️  {} has not pushed in {} (last seen {}).",
                safe_cell(m.display_name()),
                humanize_secs(m.seconds_since_last_seen),
                safe_cell(m.last_seen_at.as_deref().unwrap_or("unknown")),
            ));
        }
    }
    if !notes.is_empty() {
        out.push('\n');
        for note in notes {
            out.push_str(&note);
            out.push('\n');
        }
    }

    out
}

fn render_row(cells: &[String], widths: &[usize], right_aligned: &[bool]) -> String {
    let mut line = String::new();
    for (i, cell) in cells.iter().enumerate() {
        let pad = widths[i].saturating_sub(cell.chars().count());
        if i + 1 == cells.len() {
            // No trailing padding on the last column, so rows have no invisible tail.
            line.push_str(cell);
        } else if right_aligned[i] {
            line.push_str(&" ".repeat(pad));
            line.push_str(cell);
            line.push_str("  ");
        } else {
            line.push_str(cell);
            line.push_str(&" ".repeat(pad + 2));
        }
    }
    line.push('\n');
    line
}

/// `missing` shouts, because it is the state nobody noticed for two days.
///
/// Unrecognised values are sanitized rather than trusted — `status` is server data like any
/// other field here.
fn status_cell(status: &str) -> String {
    match status {
        "active" => "active".to_string(),
        "stale" => "STALE".to_string(),
        "missing" => "MISSING".to_string(),
        other => safe_cell(other),
    }
}

/// Longest a single cell may be. A machine cannot blow up the table by reporting a
/// pathological hostname.
const MAX_CELL_CHARS: usize = 64;

/// Make server-supplied text safe to print to a terminal.
///
/// Every string in this table originates on a machine that pushed to the org — hostnames,
/// labels and cursor keys are all attacker-influenceable by any enrolled box. A table whose
/// entire value is "you can trust this readout" must not let one machine repaint another's
/// row, so strip control characters and bound the width. Anything left renders as inert
/// literal text.
///
/// `char::is_control()` is the Unicode `Cc` category, which is C0 *and* C1
/// (U+0000–U+001F, U+007F–U+009F). That covers `ESC`, and with it every CSI escape
/// sequence — no separate C1 range check is needed.
///
/// `--json` output is deliberately left untouched: it is not going to a terminal, and
/// sanitizing it would corrupt the data a caller is parsing.
fn safe_cell(raw: &str) -> String {
    let cleaned: String = raw.chars().filter(|c| !c.is_control()).collect();
    if cleaned.chars().count() > MAX_CELL_CHARS {
        let truncated: String = cleaned.chars().take(MAX_CELL_CHARS - 1).collect();
        return format!("{truncated}…");
    }
    cleaned
}

/// Cursor keys `push` actually sends (`cloud.rs`, `IngestRequest.cursors`). Rendered first
/// so the real watermark survives any budget applied to an over-stuffed cursor object.
const KNOWN_CURSOR_KEYS: [&str; 2] = ["history_id", "trajectory_rowid"];

/// Most cursor entries to render before summarising the rest.
const MAX_CURSOR_ENTRIES: usize = 6;

/// Render the cursor map as `key=value` pairs, bounded without losing the diagnostic.
///
/// Three constraints pull against each other here, and the earlier attempts each dropped
/// one:
///
/// - Bound the output. `cursors` is stored as free-form jsonb from whatever the client
///   sent, so a machine can put arbitrarily many keys in its own row.
/// - Never *silently* drop a key. Capping the joined string did exactly that — an entry
///   sorting before `history_id` could push it out with nothing to show it had happened.
/// - Keep the watermark visible. `history_id` is the number this column exists for.
///
/// So: known cursor keys first, then the rest alphabetically, bounded by entry count with
/// any remainder stated explicitly rather than vanishing. Each key and value is also
/// bounded individually, so one absurd value cannot crowd out its neighbours.
///
/// The bound applies to the *work* as well as the output. Sorting the whole key set and then
/// truncating would let one enrolled machine decide how much CPU every operator's `coverage`
/// spends, so the entries actually rendered are selected in a single bounded pass and the
/// remainder is only counted.
fn format_cursors(cursors: &serde_json::Value) -> String {
    let Some(map) = cursors.as_object().filter(|m| !m.is_empty()) else {
        return "-".to_string();
    };

    let known: Vec<&String> = KNOWN_CURSOR_KEYS
        .iter()
        .filter_map(|k| map.get_key_value(*k).map(|(k, _)| k))
        .collect();

    // Keep the `budget` lexicographically smallest unknown keys; never hold more than that.
    let budget = MAX_CURSOR_ENTRIES.saturating_sub(known.len());
    let mut rest: Vec<&String> = Vec::with_capacity(budget);
    let mut omitted = 0usize;
    for key in map.keys() {
        if KNOWN_CURSOR_KEYS.contains(&key.as_str()) {
            continue;
        }
        let at = rest.partition_point(|held| *held < key);
        if at >= budget {
            omitted += 1;
            continue;
        }
        rest.insert(at, key);
        if rest.len() > budget {
            rest.pop();
            omitted += 1;
        }
    }

    let parts: Vec<String> = known
        .into_iter()
        .chain(rest)
        .map(|key| {
            let value = map.get(key).map(compact_json).unwrap_or_default();
            format!("{}={}", safe_cell(key), safe_cell(&value))
        })
        .chain((omitted > 0).then(|| format!("(+{omitted} more)")))
        .collect();

    parts.join(" ")
}

fn compact_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Coarse, human-scale durations. Precision past the unit is noise for this question.
fn humanize_secs(secs: u64) -> String {
    match secs {
        0..=89 => format!("{secs}s"),
        90..=5399 => format!("{}m", secs / 60),
        5400..=172_799 => format!("{}h", secs / 3600),
        _ => format!("{}d", secs / 86_400),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_hist_core::{init_db, insert_history, HistoryEntry};
    use std::cell::RefCell;

    fn mem() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn
    }

    fn add(conn: &Connection, prompt: &str, ts: i64) {
        add_for_session(conn, "s1", prompt, ts);
    }

    fn add_for_session(conn: &Connection, session: &str, prompt: &str, ts: i64) {
        insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some(session.into()),
                project: None,
                prompt: prompt.into(),
                prompt_hash: Some(ai_hist_core::prompt_hash(prompt)),
                timestamp_ms: ts,
            },
        )
        .unwrap();
    }

    fn add_trajectory(conn: &Connection, id: &str) {
        conn.execute(
            "INSERT INTO trajectories (id, version, persona_id, project_id, task_title, \
             task_description, status, decisions_json, retrospective_json, search_text, path, \
             updated_ms, timestamp_ms) VALUES (?,1,?,?,?,?,?,?,?,?,NULL,?,?)",
            rusqlite::params![
                id,
                "planner",
                "proj",
                "Build forms",
                "desc",
                "completed",
                "[]",
                format!(r#"{{"summary":"summary {id}"}}"#),
                "search",
                1,
                1_782_036_000_000i64
            ],
        )
        .unwrap();
    }

    /// Captures the request and returns a canned response.
    struct FakeIngestor {
        last: RefCell<Option<IngestRequest>>,
    }
    impl Ingestor for FakeIngestor {
        fn ingest(&self, _auth: &StoredAuth, req: &IngestRequest) -> Result<IngestResponse> {
            *self.last.borrow_mut() = Some(req.clone());
            Ok(IngestResponse {
                batch_id: req.batch_id.clone(),
                received: req.records.len() as u64,
                accepted: req.records.len() as u64,
                cursors: None,
            })
        }
    }

    /// Simulates the Worker rejecting a request whose encoded records are too large. It records
    /// every attempt so the test verifies we retry from the same cursor at a smaller limit.
    struct CapacityLimitedIngestor {
        max_records: usize,
        attempted_record_counts: RefCell<Vec<usize>>,
    }

    impl Ingestor for CapacityLimitedIngestor {
        fn ingest(&self, _auth: &StoredAuth, req: &IngestRequest) -> Result<IngestResponse> {
            self.attempted_record_counts
                .borrow_mut()
                .push(req.records.len());
            if req.records.len() > self.max_records {
                return Err(ingest_status_error(
                    503,
                    "Worker exceeded resource limits".to_string(),
                    req.records.len(),
                ));
            }
            Ok(IngestResponse {
                batch_id: req.batch_id.clone(),
                received: req.records.len() as u64,
                accepted: req.records.len() as u64,
                cursors: None,
            })
        }
    }

    struct GenericUnavailableIngestor {
        attempts: RefCell<usize>,
    }

    impl Ingestor for GenericUnavailableIngestor {
        fn ingest(&self, _auth: &StoredAuth, _req: &IngestRequest) -> Result<IngestResponse> {
            *self.attempts.borrow_mut() += 1;
            Err(ingest_status_error(
                503,
                "service temporarily unavailable".to_string(),
                32,
            ))
        }
    }

    // RELAYHISTORY_HOME is process-global; serialize env-home tests so cargo's parallel
    // runner can't clobber it across tests.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            std::env::remove_var(key);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.previous {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn with_temp_home<T>(f: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _relayhistory_home = EnvVarGuard::set("RELAYHISTORY_HOME", dir.path());
        f()
    }

    fn read_http_request(
        stream: &mut std::net::TcpStream,
    ) -> (String, std::collections::HashMap<String, String>, String) {
        use std::io::{BufRead, BufReader, Read};

        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut first_line = String::new();
        reader.read_line(&mut first_line).unwrap();
        let mut headers = std::collections::HashMap::new();
        let mut content_length = 0usize;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                break;
            }
            if let Some((name, value)) = trimmed.split_once(':') {
                let name = name.to_ascii_lowercase();
                let value = value.trim().to_string();
                if name == "content-length" {
                    content_length = value.parse().unwrap();
                }
                headers.insert(name, value);
            }
        }
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).unwrap();
        (first_line, headers, String::from_utf8(body).unwrap())
    }

    fn write_http_response(stream: &mut std::net::TcpStream, status: &str, body: &str) {
        use std::io::Write;

        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
    }

    #[test]
    fn write_private_replaces_existing_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cursor.json");

        write_private(&path, r#"{"history_id":1}"#).unwrap();
        write_private(&path, r#"{"history_id":2}"#).unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), r#"{"history_id":2}"#);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn push_sends_batch_advances_and_persists_cursor() {
        with_temp_home(|| {
            let conn = mem();
            add(&conn, "first", 1);
            add(&conn, "second", 2);
            let client = FakeIngestor {
                last: RefCell::new(None),
            };
            let auth = StoredAuth {
                base_url: "http://localhost:8787".into(),
                access_token: "rth_at_test".into(),
                ..Default::default()
            };
            let machine = MachineIdentity {
                id: "m1".into(),
                ..Default::default()
            };
            let report = push(
                &conn,
                &client,
                &auth,
                &machine,
                &SyncCursor::default(),
                100,
                &HashSet::new(),
            )
            .unwrap();
            assert_eq!(report.sent, 2);
            assert_eq!(report.accepted, 2);
            assert_eq!(report.cursor.history_id, 2);
            // request carried the deterministic batch id + machine + records
            let sent = client.last.borrow().clone().unwrap();
            assert_eq!(sent.machine.id, "m1");
            assert!(sent.batch_id.starts_with("b_"));
            assert_eq!(sent.records.len(), 2);
            // cursor persisted to disk and reloads to the advanced value
            assert_eq!(load_cursor(&auth.base_url).unwrap().history_id, 2);
        });
    }

    #[test]
    fn push_shrinks_from_the_emitted_batch_size_and_makes_progress() {
        with_temp_home(|| {
            let conn = mem();
            for id in 1..=32 {
                add(&conn, &format!("entry {id}"), id);
            }
            let client = CapacityLimitedIngestor {
                max_records: 10,
                attempted_record_counts: RefCell::new(Vec::new()),
            };
            let auth = StoredAuth {
                base_url: "http://localhost:8787".into(),
                access_token: "test-access-token".into(),
                ..Default::default()
            };
            let report = push(
                &conn,
                &client,
                &auth,
                &MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                &SyncCursor::default(),
                500,
                &HashSet::new(),
            )
            .unwrap();

            // The configured limit is 500 but only 32 rows are pending. Retrying from 250
            // would resend the same 32-record payload; shrink from the emitted size instead.
            assert_eq!(*client.attempted_record_counts.borrow(), vec![32, 16, 8]);
            assert_eq!(report.batch_limit, 8);
            assert_eq!(report.attempts, 3);
            assert_eq!(report.cursor.history_id, 8);
            assert_eq!(load_cursor(&auth.base_url).unwrap().history_id, 8);
        });
    }

    #[test]
    fn capacity_retry_strictly_reduces_a_per_source_limit() {
        with_temp_home(|| {
            let conn = mem();
            for id in 1..=4 {
                add(&conn, &format!("entry {id}"), id);
                add_trajectory(&conn, &format!("trajectory-{id}"));
            }
            let client = CapacityLimitedIngestor {
                max_records: 6,
                attempted_record_counts: RefCell::new(Vec::new()),
            };
            let auth = StoredAuth {
                base_url: "http://localhost:8787".into(),
                access_token: "test-access-token".into(),
                ..Default::default()
            };
            let report = push(
                &conn,
                &client,
                &auth,
                &MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                &SyncCursor::default(),
                4,
                &HashSet::new(),
            )
            .unwrap();

            // Four rows from each source emit eight records. Halving the emitted count would
            // leave the per-source limit at four and retry those same eight forever; the retry
            // must lower it to two and make progress.
            assert_eq!(*client.attempted_record_counts.borrow(), vec![8, 4]);
            assert_eq!(report.batch_limit, 2);
            assert_eq!(report.attempts, 2);
            assert_eq!(report.cursor.history_id, 2);
            assert_eq!(report.cursor.trajectory_rowid, 2);
        });
    }

    #[test]
    fn cursor_saves_merge_monotonically_across_concurrent_pushes() {
        with_temp_home(|| {
            let base_url = "http://localhost:8787";
            save_cursor(
                base_url,
                &SyncCursor {
                    history_id: 100,
                    trajectory_rowid: 1,
                    trajectory_updated_ms: 50,
                    commit_link_id: 2,
                    ..Default::default()
                },
            )
            .unwrap();
            // Even a later stale writer cannot rewind history/commit ids, or the
            // trajectory keyset position (updated_ms, rowid) as a single pair.
            save_cursor(
                base_url,
                &SyncCursor {
                    history_id: 1,
                    trajectory_rowid: 100,
                    trajectory_updated_ms: 5,
                    commit_link_id: 20,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(
                load_cursor(base_url).unwrap(),
                SyncCursor {
                    history_id: 100,
                    trajectory_rowid: 1,
                    trajectory_updated_ms: 50,
                    commit_link_id: 20,
                    ..Default::default()
                }
            );

            let mut writers = Vec::new();
            for writer in 0..8 {
                writers.push(std::thread::spawn(move || {
                    for revision in 1..=25 {
                        let cursor = if writer % 2 == 0 {
                            SyncCursor {
                                history_id: 100 + revision,
                                trajectory_rowid: 1,
                                ..Default::default()
                            }
                        } else {
                            SyncCursor {
                                history_id: 1,
                                trajectory_rowid: 100 + revision,
                                ..Default::default()
                            }
                        };
                        save_cursor("http://localhost:8787", &cursor).unwrap();
                    }
                }));
            }
            for writer in writers {
                writer.join().unwrap();
            }
            assert_eq!(
                load_cursor(base_url).unwrap(),
                SyncCursor {
                    history_id: 125,
                    trajectory_rowid: 1,
                    trajectory_updated_ms: 50,
                    commit_link_id: 20,
                    ..Default::default()
                }
            );
        });
    }

    #[test]
    fn empty_reduced_retry_persists_skipped_source_cursors() {
        with_temp_home(|| {
            let conn = mem();
            add_for_session(&conn, "private-history", "private", 1);
            add_for_session(&conn, "public-history", "public", 2);
            add_trajectory(&conn, "private-trajectory");
            add_trajectory(&conn, "public-trajectory");
            let incognito: HashSet<String> = [
                "private-history".to_string(),
                "private-trajectory".to_string(),
            ]
            .into_iter()
            .collect();
            let client = CapacityLimitedIngestor {
                max_records: 1,
                attempted_record_counts: RefCell::new(Vec::new()),
            };
            let auth = StoredAuth {
                base_url: "http://localhost:8787".into(),
                access_token: "test-access-token".into(),
                ..Default::default()
            };
            let report = push(
                &conn,
                &client,
                &auth,
                &MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                &SyncCursor::default(),
                2,
                &incognito,
            )
            .unwrap();

            // The first request contains the two public rows and is rejected. Limit one then
            // scans only the two private rows, producing no records but advancing both cursors.
            assert_eq!(*client.attempted_record_counts.borrow(), vec![2]);
            assert_eq!(report.sent, 0);
            assert_eq!(report.attempts, 1);
            assert_eq!(report.cursor.history_id, 1);
            assert_eq!(report.cursor.trajectory_rowid, 1);
            assert_eq!(load_cursor(&auth.base_url).unwrap(), report.cursor);

            // A later run starts after the skipped rows and can send the public rows instead of
            // reconstructing the original rejected span.
            let accepting = CapacityLimitedIngestor {
                max_records: 2,
                attempted_record_counts: RefCell::new(Vec::new()),
            };
            let next = push(
                &conn,
                &accepting,
                &auth,
                &MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                &report.cursor,
                2,
                &incognito,
            )
            .unwrap();
            assert_eq!(*accepting.attempted_record_counts.borrow(), vec![2]);
            assert_eq!(next.sent, 2);
            assert_eq!(next.cursor.history_id, 2);
            assert_eq!(next.cursor.trajectory_rowid, 2);
        });
    }

    #[test]
    fn an_unrelated_503_is_not_retried_as_a_capacity_failure() {
        with_temp_home(|| {
            let conn = mem();
            for id in 1..=32 {
                add(&conn, &format!("entry {id}"), id);
            }
            let client = GenericUnavailableIngestor {
                attempts: RefCell::new(0),
            };
            let auth = StoredAuth {
                base_url: "http://localhost:8787".into(),
                access_token: "test-access-token".into(),
                ..Default::default()
            };
            let err = push(
                &conn,
                &client,
                &auth,
                &MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                &SyncCursor::default(),
                500,
                &HashSet::new(),
            )
            .unwrap_err();
            let message = format!("{err:#}");

            assert!(message.contains("temporarily unavailable"), "{message}");
            assert_eq!(*client.attempts.borrow(), 1);
            assert_eq!(load_cursor(&auth.base_url).unwrap(), SyncCursor::default());
        });
    }

    #[test]
    fn worker_rejection_at_the_minimum_limit_is_loud_and_keeps_the_cursor() {
        with_temp_home(|| {
            let conn = mem();
            add(&conn, "too large even alone", 1);
            let client = CapacityLimitedIngestor {
                max_records: 0,
                attempted_record_counts: RefCell::new(Vec::new()),
            };
            let auth = StoredAuth {
                base_url: "http://localhost:8787".into(),
                access_token: "test-access-token".into(),
                ..Default::default()
            };
            let err = push(
                &conn,
                &client,
                &auth,
                &MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                &SyncCursor::default(),
                1,
                &HashSet::new(),
            )
            .unwrap_err()
            .to_string();

            assert!(err.contains("cursor was not advanced"), "{err}");
            assert_eq!(*client.attempted_record_counts.borrow(), vec![1]);
            assert_eq!(load_cursor(&auth.base_url).unwrap(), SyncCursor::default());
        });
    }

    #[test]
    fn push_is_noop_when_nothing_new() {
        with_temp_home(|| {
            let conn = mem();
            let client = FakeIngestor {
                last: RefCell::new(None),
            };
            let report = push(
                &conn,
                &client,
                &StoredAuth::default(),
                &MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                &SyncCursor::default(),
                100,
                &HashSet::new(),
            )
            .unwrap();
            assert_eq!(report.sent, 0);
            assert!(report.batch_id.is_none());
            assert!(client.last.borrow().is_none()); // no HTTP call made
        });
    }

    #[test]
    fn batch_id_is_deterministic_for_same_span() {
        let from = SyncCursor::default();
        let to = SyncCursor {
            history_id: 5,
            trajectory_rowid: 2,
            ..Default::default()
        };
        assert_eq!(batch_id("m1", &from, &to, 7), batch_id("m1", &from, &to, 7));
        assert_ne!(batch_id("m1", &from, &to, 7), batch_id("m1", &from, &to, 8));
        assert_ne!(batch_id("m2", &from, &to, 7), batch_id("m1", &from, &to, 7));
    }

    #[test]
    fn auth_and_cursor_round_trip_on_disk() {
        with_temp_home(|| {
            let auth = StoredAuth {
                base_url: "http://localhost:8787".into(),
                access_token: "rth_at_x".into(),
                refresh_token: Some("rth_rt_y".into()),
                org_id: Some("org-a".into()),
                workspace_id: None,
            };
            save_auth(&auth).unwrap();
            assert_eq!(load_auth(Some(&auth.base_url)).unwrap().unwrap(), auth);
            let c = SyncCursor {
                history_id: 9,
                trajectory_rowid: 4,
                ..Default::default()
            };
            save_cursor(&auth.base_url, &c).unwrap();
            assert_eq!(load_cursor(&auth.base_url).unwrap(), c);
        });
    }

    #[test]
    fn concurrent_auth_saves_never_publish_partial_json() {
        with_temp_home(|| {
            let mut writers = Vec::new();
            for writer in 0..8 {
                writers.push(std::thread::spawn(move || {
                    for revision in 0..40 {
                        save_auth(&StoredAuth {
                            base_url: "http://localhost:8787".into(),
                            access_token: format!("rth_at_{writer}_{revision}"),
                            refresh_token: Some(format!("rth_rt_{writer}_{revision}")),
                            ..Default::default()
                        })
                        .unwrap();
                    }
                }));
            }
            for writer in writers {
                writer.join().unwrap();
            }

            let stored = load_auth(Some("http://localhost:8787")).unwrap().unwrap();
            assert!(stored.access_token.starts_with("rth_at_"));
            assert!(stored
                .refresh_token
                .as_deref()
                .is_some_and(|token| token.starts_with("rth_rt_")));
            let leftovers: Vec<_> = fs::read_dir(stage_dir())
                .unwrap()
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp."))
                .collect();
            assert!(
                leftovers.is_empty(),
                "leftover auth temp files: {leftovers:?}"
            );
        });
    }

    #[test]
    fn concurrent_401s_share_one_rotated_refresh_token() {
        with_temp_home(|| {
            use std::sync::{Arc, Barrier};

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let addr = listener.local_addr().unwrap();
            let refreshes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let server_refreshes = Arc::clone(&refreshes);
            let server = std::thread::spawn(move || {
                let started = std::time::Instant::now();
                let mut handled = 0;
                while handled < 5 && started.elapsed() < std::time::Duration::from_secs(10) {
                    let (mut stream, _) = match listener.accept() {
                        Ok(connection) => connection,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(5));
                            continue;
                        }
                        Err(error) => panic!("accept failed: {error}"),
                    };
                    handled += 1;
                    let (line, headers, body) = read_http_request(&mut stream);
                    if line.starts_with("POST /v1/auth/token/refresh ") {
                        assert_eq!(
                            serde_json::from_str::<serde_json::Value>(&body).unwrap()
                                ["refreshToken"],
                            "rth_rt_old"
                        );
                        let prior = server_refreshes.fetch_add(1, Ordering::SeqCst);
                        if prior == 0 {
                            write_http_response(
                                &mut stream,
                                "200 OK",
                                r#"{"accessToken":"rth_at_fresh","refreshToken":"rth_rt_fresh"}"#,
                            );
                        } else {
                            write_http_response(
                                &mut stream,
                                "401 Unauthorized",
                                r#"{"error":"refresh token already rotated"}"#,
                            );
                        }
                        continue;
                    }

                    assert!(line.starts_with("POST /v1/ingest "), "{line}");
                    match headers.get("authorization").map(String::as_str) {
                        Some("Bearer rth_at_expired") => write_http_response(
                            &mut stream,
                            "401 Unauthorized",
                            r#"{"error":"invalid_token"}"#,
                        ),
                        Some("Bearer rth_at_fresh") => write_http_response(
                            &mut stream,
                            "200 OK",
                            r#"{"batchId":"b_test","received":0,"accepted":0}"#,
                        ),
                        other => panic!("unexpected authorization header: {other:?}"),
                    }
                }
                assert_eq!(
                    handled, 5,
                    "expected two initial calls, one refresh, two retries"
                );
            });

            let auth = StoredAuth {
                base_url: format!("http://{addr}"),
                access_token: "rth_at_expired".into(),
                refresh_token: Some("rth_rt_old".into()),
                org_id: Some("org-a".into()),
                workspace_id: Some("ws-a".into()),
            };
            save_auth(&auth).unwrap();
            let barrier = Arc::new(Barrier::new(3));
            let mut clients = Vec::new();
            for _ in 0..2 {
                let auth = auth.clone();
                let barrier = Arc::clone(&barrier);
                clients.push(std::thread::spawn(move || {
                    barrier.wait();
                    UreqIngestor.ingest(
                        &auth,
                        &IngestRequest {
                            machine: MachineIdentity {
                                id: "m1".into(),
                                ..Default::default()
                            },
                            batch_id: "b_test".into(),
                            cursors: None,
                            records: Vec::new(),
                        },
                    )
                }));
            }
            barrier.wait();
            for client in clients {
                assert_eq!(client.join().unwrap().unwrap().batch_id, "b_test");
            }
            server.join().unwrap();

            assert_eq!(refreshes.load(Ordering::SeqCst), 1);
            let stored = load_auth(Some(&auth.base_url)).unwrap().unwrap();
            assert_eq!(stored.access_token, "rth_at_fresh");
            assert_eq!(stored.refresh_token.as_deref(), Some("rth_rt_fresh"));
        });
    }

    #[test]
    fn a_reused_auth_value_adopts_the_persisted_rotation() {
        with_temp_home(|| {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let addr = listener.local_addr().unwrap();
            let stale_requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let refreshes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let server_stale_requests = std::sync::Arc::clone(&stale_requests);
            let server_refreshes = std::sync::Arc::clone(&refreshes);
            let server = std::thread::spawn(move || {
                let started = std::time::Instant::now();
                let mut handled = 0;
                while handled < 4 && started.elapsed() < std::time::Duration::from_secs(10) {
                    let (mut stream, _) = match listener.accept() {
                        Ok(connection) => connection,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(5));
                            continue;
                        }
                        Err(error) => panic!("accept failed: {error}"),
                    };
                    handled += 1;
                    let (line, headers, _) = read_http_request(&mut stream);
                    if line.starts_with("POST /v1/auth/token/refresh ") {
                        server_refreshes.fetch_add(1, Ordering::SeqCst);
                        write_http_response(
                            &mut stream,
                            "200 OK",
                            r#"{"accessToken":"rth_at_fresh","refreshToken":"rth_rt_fresh"}"#,
                        );
                        continue;
                    }

                    assert!(line.starts_with("POST /v1/ingest "), "{line}");
                    match headers.get("authorization").map(String::as_str) {
                        Some("Bearer rth_at_expired") => {
                            server_stale_requests.fetch_add(1, Ordering::SeqCst);
                            write_http_response(
                                &mut stream,
                                "401 Unauthorized",
                                r#"{"error":"invalid_token"}"#,
                            );
                        }
                        Some("Bearer rth_at_fresh") => write_http_response(
                            &mut stream,
                            "200 OK",
                            r#"{"batchId":"b_test","received":0,"accepted":0}"#,
                        ),
                        other => panic!("unexpected authorization header: {other:?}"),
                    }
                }
                assert_eq!(
                    handled, 4,
                    "expected one stale call, one refresh, and two successful calls"
                );
            });

            let auth = StoredAuth {
                base_url: format!("http://{addr}"),
                access_token: "rth_at_expired".into(),
                refresh_token: Some("rth_rt_old".into()),
                ..Default::default()
            };
            save_auth(&auth).unwrap();
            let request = IngestRequest {
                machine: MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                batch_id: "b_test".into(),
                cursors: None,
                records: Vec::new(),
            };

            UreqIngestor.ingest(&auth, &request).unwrap();
            // Deliberately reuse the stale value. The persisted rotated pair must be selected
            // before sending, without another guaranteed-to-fail request or refresh.
            UreqIngestor.ingest(&auth, &request).unwrap();
            server.join().unwrap();

            assert_eq!(stale_requests.load(Ordering::SeqCst), 1);
            assert_eq!(refreshes.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn equivalent_url_forms_share_one_stage_store() {
        with_temp_home(|| {
            let auth = StoredAuth {
                base_url: "http://LocalHost:8787/".into(),
                access_token: "rth_at_equivalent".into(),
                ..Default::default()
            };
            save_auth(&auth).unwrap();
            let cursor = SyncCursor {
                history_id: 17,
                trajectory_rowid: 3,
                ..Default::default()
            };
            save_cursor(&auth.base_url, &cursor).unwrap();

            let stored = load_auth(Some("http://localhost:8787")).unwrap().unwrap();
            assert_eq!(stored.base_url, "http://localhost:8787");
            assert_eq!(stored.access_token, auth.access_token);
            assert_eq!(load_cursor(" HTTP://LOCALHOST:8787/ ").unwrap(), cursor);
            assert_eq!(
                auth_path("http://LocalHost:8787/").unwrap(),
                auth_path("http://localhost:8787").unwrap()
            );
            assert_eq!(
                auth_path("https://History.AgentRelay.com:443/").unwrap(),
                auth_path("https://history.agentrelay.com").unwrap(),
                "an explicit default HTTPS port must not create another stage"
            );
            assert_eq!(
                auth_path("http://Example.test:80/").unwrap(),
                auth_path("http://example.test").unwrap(),
                "an explicit default HTTP port must not create another stage"
            );
        });
    }

    #[test]
    fn legacy_state_cannot_make_an_unqualified_command_guess_or_leak_a_cursor() {
        with_temp_home(|| {
            let prod = StoredAuth {
                base_url: "https://history.agentrelay.com".into(),
                access_token: "rth_at_prod".into(),
                ..Default::default()
            };
            save_auth(&prod).unwrap();

            let legacy_dev = StoredAuth {
                base_url: "http://localhost:8787".into(),
                access_token: "rth_at_dev".into(),
                ..Default::default()
            };
            write_private(
                &legacy_auth_path(),
                &serde_json::to_string_pretty(&legacy_dev).unwrap(),
            )
            .unwrap();
            let legacy_cursor = SyncCursor {
                history_id: 900,
                trajectory_rowid: 42,
                ..Default::default()
            };
            write_private(
                &legacy_cursor_path(),
                &serde_json::to_string_pretty(&legacy_cursor).unwrap(),
            )
            .unwrap();

            let err = load_auth(None).unwrap_err().to_string();
            assert!(err.contains("2 relayhistory stages"), "{err}");
            assert_eq!(
                load_cursor(&prod.base_url).unwrap(),
                SyncCursor::default(),
                "a prod push must not inherit the dev cursor"
            );
            assert_eq!(
                load_cursor("http://LOCALHOST:8787/").unwrap(),
                legacy_cursor,
                "the legacy cursor remains available to its own normalized stage"
            );
        });
    }

    /// A login to a second stage must preserve both the original stage's bearer session and
    /// its outbox watermark. The previous single auth.json/cursor.json layout overwrote both.
    #[test]
    fn auth_and_cursor_are_scoped_to_the_cloud_stage() {
        with_temp_home(|| {
            let prod = StoredAuth {
                base_url: "https://history.agentrelay.com".into(),
                access_token: "test-prod-access".into(),
                ..Default::default()
            };
            let dev = StoredAuth {
                base_url: "http://localhost:8787".into(),
                access_token: "test-dev-access".into(),
                ..Default::default()
            };
            let prod_cursor = SyncCursor {
                history_id: 101,
                trajectory_rowid: 7,
                ..Default::default()
            };
            let dev_cursor = SyncCursor {
                history_id: 9,
                trajectory_rowid: 2,
                ..Default::default()
            };

            save_auth(&prod).unwrap();
            save_cursor(&prod.base_url, &prod_cursor).unwrap();
            save_auth(&dev).unwrap();
            save_cursor(&dev.base_url, &dev_cursor).unwrap();

            assert_eq!(load_auth(Some(&prod.base_url)).unwrap(), Some(prod));
            assert_eq!(
                load_cursor("https://history.agentrelay.com").unwrap(),
                prod_cursor
            );
            assert_eq!(load_auth(Some(&dev.base_url)).unwrap(), Some(dev));
            assert_eq!(load_cursor("http://localhost:8787").unwrap(), dev_cursor);

            let err = load_auth(None).unwrap_err().to_string();
            assert!(err.contains("pass --base-url"), "{err}");
        });
    }

    #[test]
    fn machine_id_is_stable_across_calls() {
        with_temp_home(|| {
            let a = machine_id().unwrap();
            let b = machine_id().unwrap();
            assert_eq!(a, b);
            assert!(a.starts_with("m_"));
        });
    }

    /// The bug this closes: `launchd` does not export `HOSTNAME`, so the scheduled push —
    /// the only one that runs unattended — reported no name and landed as an anonymous row.
    /// Every machine in `coverage` was therefore identified by an opaque `m_…` id.
    #[test]
    fn hostname_is_resolved_when_the_environment_does_not_supply_one() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _hostname = EnvVarGuard::remove("HOSTNAME");

        let resolved = machine_hostname().expect("the OS must name this machine");
        assert!(!resolved.trim().is_empty());
        assert_ne!(resolved, "unknown-host");
        assert_eq!(hostname(), resolved);
    }

    /// Kept as an override so the resolution stays testable and a box can be labelled.
    #[test]
    fn hostname_env_override_wins_and_an_empty_one_falls_through() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        {
            let _hostname = EnvVarGuard::set("HOSTNAME", "kjg-laptop");
            assert_eq!(machine_hostname().as_deref(), Some("kjg-laptop"));
        }
        // A blank or whitespace-only `HOSTNAME` is not a name; fall through to the OS
        // rather than pushing an empty string as this machine's identity.
        let _blank = EnvVarGuard::set("HOSTNAME", "   ");
        let resolved = machine_hostname().expect("the OS must name this machine");
        assert!(!resolved.trim().is_empty());
    }

    #[test]
    fn pair_request_serializes_camelcase_and_omits_empties() {
        let ctx = PairContext {
            project_id: Some("proj-auth-svc".into()),
            repo_path: Some("/Users/~/Projects/x".into()),
            cwd: None,
            git_remote: None,
            task: Some("refactor auth middleware".into()),
            files: vec!["src/auth/mw.ts".into()],
            tool: Some("Edit".into()),
            target: None,
            recent_prompt: None,
        };
        let v = serde_json::to_value(PairRequest {
            context: &ctx,
            limit: 5,
        })
        .unwrap();
        let c = &v["context"];
        assert_eq!(c["projectId"], "proj-auth-svc");
        assert_eq!(c["repoPath"], "/Users/~/Projects/x");
        assert_eq!(c["task"], "refactor auth middleware");
        assert_eq!(c["files"][0], "src/auth/mw.ts");
        assert_eq!(c["tool"], "Edit");
        assert_eq!(v["limit"], 5);
        // None/empty fields must be absent on the wire, not null.
        assert!(c.get("cwd").is_none());
        assert!(c.get("gitRemote").is_none());
        assert!(c.get("target").is_none());
        assert!(c.get("recentPrompt").is_none());
    }

    #[test]
    fn pair_response_parses_warn_with_evidence() {
        let resp: PairResponse = serde_json::from_str(
            r#"{"decision":"warn","correlationId":"c_1","warnings":[
                {"text":"prior auth refactor broke token refresh","kind":"reflection",
                 "lens":"trajectories","score":0.87,
                 "evidence":[{"machineId":"m_x","source":"trajectories","sessionId":"s1",
                   "kind":"reflection","eventId":"reflection:tA:suggestion:0","ts":"2026-06-01T00:00:00Z",
                   "snippet":"update permissions when editing mw"}]}]}"#,
        )
        .unwrap();
        assert_eq!(resp.decision.as_deref(), Some("warn"));
        assert_eq!(resp.warnings.len(), 1);
        let w = &resp.warnings[0];
        assert_eq!(
            w.evidence[0].event_id.as_deref(),
            Some("reflection:tA:suggestion:0")
        );
        let rendered = format_pair_warnings(&resp);
        assert!(rendered.contains("Pair warning(s)"));
        assert!(rendered.contains("reflection:tA:suggestion:0"));
    }

    fn coverage_machine(host: &str, status: &str, secs: u64, batches: u64) -> MachineCoverage {
        MachineCoverage {
            machine_id: format!("m_{host}"),
            hostname: Some(host.to_string()),
            last_seen_at: Some("2026-08-03T04:11:00Z".to_string()),
            seconds_since_last_seen: secs,
            status: status.to_string(),
            cursors: serde_json::json!({ "history_id": 82_896 }),
            recent: CoverageRecent {
                batches,
                records: batches * 4,
                accepted: batches * 4,
                last_batch_at: Some("2026-08-06T07:00:00Z".to_string()),
                last_batch_record_count: Some(4),
            },
            ..Default::default()
        }
    }

    /// The CLI half of the finn-mini failure: a machine that stopped pushing has to be
    /// impossible to skim past. It keeps its row, gets a loud status, and is named again
    /// in prose beneath the table.
    #[test]
    fn coverage_table_calls_out_a_machine_that_stopped_pushing() {
        let resp = CoverageResponse {
            machines: vec![
                coverage_machine("kjg-laptop", "active", 120, 280),
                coverage_machine("sf-mini", "active", 240, 275),
                coverage_machine("finn-mini", "missing", 3 * 86_400, 0),
            ],
            summary: CoverageSummary {
                total: 3,
                active: 2,
                stale: 0,
                missing: 1,
            },
            thresholds: CoverageThresholds {
                stale_after_seconds: 900,
                missing_after_seconds: 86_400,
                window_hours: 24,
            },
        };

        let out = format_fleet_coverage(&resp);

        assert!(
            out.contains("3 machine(s): 2 active, 0 stale, 1 missing"),
            "{out}"
        );
        assert!(out.contains("MISSING"), "{out}");
        assert!(
            out.contains("⚠️  finn-mini has not pushed in 3d"),
            "a mute machine must be named in prose, not only in a column: {out}"
        );
        // The machines that are fine must not be dressed up as problems.
        assert!(!out.contains("kjg-laptop has not pushed"), "{out}");
        assert!(!out.contains("sf-mini has not pushed"), "{out}");
        assert!(out.contains("history_id=82896"), "{out}");
        assert!(resp.has_gaps());
    }

    #[test]
    fn coverage_table_stays_quiet_when_the_whole_fleet_is_reporting() {
        let resp = CoverageResponse {
            machines: vec![
                coverage_machine("kjg-laptop", "active", 120, 280),
                coverage_machine("finn-mini", "active", 90, 279),
            ],
            summary: CoverageSummary {
                total: 2,
                active: 2,
                ..Default::default()
            },
            thresholds: CoverageThresholds {
                stale_after_seconds: 900,
                missing_after_seconds: 86_400,
                window_hours: 24,
            },
        };

        let out = format_fleet_coverage(&resp);
        assert!(!out.contains("⚠️"), "{out}");
        assert!(!out.contains("MISSING"), "{out}");
        assert!(!resp.has_gaps());
    }

    /// A full batch is not evidence of anything, and coverage must not narrate it as if it
    /// were. Both machines below hit their limit; neither is behind.
    #[test]
    fn coverage_table_does_not_infer_a_backlog_from_a_full_batch() {
        // Filled the 500 default exactly, and is caught up: the server accepted 0 new
        // records because it already had them all — finn-mini's real shape on 2026-08-06.
        let mut caught_up = coverage_machine("finn-mini", "active", 60, 12);
        caught_up.recent.last_batch_record_count = Some(500);
        caught_up.recent.accepted = 0;
        // Pushes with `--limit 200`, so every batch it ever completes is "full".
        let mut small_limit = coverage_machine("sf-mini", "active", 60, 12);
        small_limit.recent.last_batch_record_count = Some(200);

        let resp = CoverageResponse {
            machines: vec![caught_up, small_limit],
            summary: CoverageSummary {
                total: 2,
                active: 2,
                ..Default::default()
            },
            thresholds: CoverageThresholds {
                window_hours: 24,
                ..Default::default()
            },
        };

        let out = format_fleet_coverage(&resp);
        // No claim about backlogs, drainage, or queued history in any wording.
        for phrase in [
            "backlog",
            "draining",
            "queued",
            "batch limit",
            "filled",
            "ℹ️",
        ] {
            assert!(
                !out.contains(phrase),
                "a full batch proves nothing, so `{phrase}` must not appear: {out}"
            );
        }
        // The counts stay — they are the evidence, stated without a cause attached.
        assert!(out.contains("RECORDS"), "{out}");
        // Still reporting, so this is not a coverage gap — `--fail-on-stale` must not fire.
        assert!(!resp.has_gaps());
    }

    /// Hostnames come from the machines themselves. One box must not be able to repaint
    /// another box's row — or hide its own — by reporting an escape sequence as its name.
    #[test]
    fn coverage_table_neutralizes_terminal_escapes_in_server_supplied_names() {
        let mut hostile = coverage_machine("evil", "missing", 3 * 86_400, 0);
        hostile.hostname = Some("\u{1b}[2Kok-box\u{1b}[1;32m".to_string());
        hostile.cursors = serde_json::json!({ "history_id": "\u{1b}[31m1" });

        let out = format_fleet_coverage(&CoverageResponse {
            machines: vec![hostile],
            summary: CoverageSummary {
                total: 1,
                missing: 1,
                ..Default::default()
            },
            thresholds: CoverageThresholds::default(),
        });

        assert!(
            !out.contains('\u{1b}'),
            "raw ESC reached the terminal: {out:?}"
        );
        // Neutralized, not dropped — the row and its gap note must still be there.
        assert!(out.contains("ok-box"), "{out}");
        assert!(out.contains("MISSING"), "{out}");
        assert!(out.contains("has not pushed in 3d"), "{out}");
    }

    /// The cursor column is the diagnostic this report exists to expose. A display-safety
    /// cap on the joined string would let one long entry silently push `history_id` out —
    /// bounding each entry instead keeps every key visible.
    #[test]
    fn coverage_table_never_drops_a_cursor_key_to_a_width_cap() {
        let mut m = coverage_machine("finn-mini", "active", 30, 1);
        m.cursors = serde_json::json!({
            "aaa_padding_key": "z".repeat(200),
            "history_id": 13_409_762,
            "trajectory_rowid": 1641,
        });

        let out = format_fleet_coverage(&CoverageResponse {
            machines: vec![m],
            summary: CoverageSummary {
                total: 1,
                active: 1,
                ..Default::default()
            },
            thresholds: CoverageThresholds::default(),
        });

        // Sorted alphabetically, the padding key precedes history_id — a whole-cell cap
        // would have consumed the budget before reaching it.
        assert!(out.contains("history_id=13409762"), "{out}");
        assert!(out.contains("trajectory_rowid=1641"), "{out}");
        // The absurd value is still shortened rather than printed in full.
        assert!(!out.contains(&"z".repeat(100)), "{out}");
    }

    /// `cursors` is free-form jsonb from the client, so a machine can stuff its own row
    /// with arbitrarily many keys. Bound it — but state the remainder rather than letting
    /// keys vanish, and never at the cost of the watermark itself.
    #[test]
    fn coverage_table_bounds_an_overstuffed_cursor_without_hiding_the_watermark() {
        let mut m = coverage_machine("finn-mini", "active", 30, 1);
        let mut cursors = serde_json::Map::new();
        for i in 0..50 {
            // "aaa_*" sorts ahead of history_id, so a naive order loses the watermark.
            cursors.insert(format!("aaa_filler_{i:02}"), serde_json::json!(i));
        }
        cursors.insert("history_id".into(), serde_json::json!(13_409_762));
        cursors.insert("trajectory_rowid".into(), serde_json::json!(1641));
        m.cursors = serde_json::Value::Object(cursors);

        let out = format_fleet_coverage(&CoverageResponse {
            machines: vec![m],
            summary: CoverageSummary {
                total: 1,
                active: 1,
                ..Default::default()
            },
            thresholds: CoverageThresholds::default(),
        });

        // The watermark survives 50 keys that all sort ahead of it.
        assert!(out.contains("history_id=13409762"), "{out}");
        assert!(out.contains("trajectory_rowid=1641"), "{out}");
        // Bounded...
        assert!(!out.contains("aaa_filler_49"), "{out}");
        // ...but the omission is stated, not silent. 52 keys - 6 rendered = 46.
        assert!(out.contains("(+46 more)"), "{out}");
    }

    #[test]
    fn coverage_table_bounds_a_pathological_hostname() {
        let mut huge = coverage_machine("x", "active", 10, 1);
        huge.hostname = Some("h".repeat(500));

        let out = format_fleet_coverage(&CoverageResponse {
            machines: vec![huge],
            summary: CoverageSummary {
                total: 1,
                active: 1,
                ..Default::default()
            },
            thresholds: CoverageThresholds::default(),
        });

        assert!(
            !out.contains(&"h".repeat(100)),
            "unbounded cell widened the table"
        );
        assert!(out.contains('…'), "{out}");
    }

    #[test]
    fn coverage_response_parses_the_server_payload() {
        let resp: CoverageResponse = serde_json::from_str(
            r#"{"machines":[{"machineId":"m_abc","hostname":"finn-mini","label":null,
                 "os":"macos","relayhistoryVersion":"0.9.0","firstSeenAt":"2026-07-01T00:00:00Z",
                 "lastSeenAt":"2026-08-03T04:11:00Z","secondsSinceLastSeen":259200,
                 "status":"missing","cursors":{"history_id":13409762},
                 "recent":{"batches":0,"records":0,"accepted":0,"lastBatchAt":null,
                   "lastBatchRecordCount":null}}],
               "summary":{"total":1,"active":0,"stale":0,"missing":1},
               "thresholds":{"staleAfterSeconds":900,"missingAfterSeconds":86400,"windowHours":24},
               "generatedAt":"2026-08-06T07:00:00Z","correlationId":"corr-1"}"#,
        )
        .unwrap();

        assert_eq!(resp.machines.len(), 1);
        let m = &resp.machines[0];
        assert_eq!(m.display_name(), "finn-mini");
        assert_eq!(m.status, "missing");
        assert_eq!(m.seconds_since_last_seen, 259_200);
        assert_eq!(m.cursors["history_id"], 13_409_762);
        assert_eq!(resp.summary.missing, 1);
        assert_eq!(resp.thresholds.window_hours, 24);
        assert!(resp.has_gaps());
    }

    #[test]
    fn coverage_falls_back_to_the_machine_id_when_a_host_never_reported_one() {
        let m = MachineCoverage {
            machine_id: "m_opaque".to_string(),
            hostname: None,
            label: None,
            ..Default::default()
        };
        assert_eq!(m.display_name(), "m_opaque");

        let labelled = MachineCoverage {
            machine_id: "m_opaque".to_string(),
            hostname: Some(String::new()),
            label: Some("barry".to_string()),
            ..Default::default()
        };
        assert_eq!(labelled.display_name(), "barry");
    }

    #[test]
    fn coverage_renders_an_empty_fleet_without_a_table() {
        let out = format_fleet_coverage(&CoverageResponse::default());
        assert_eq!(out, "No machines have pushed history to this org yet.\n");
    }

    #[test]
    fn humanize_secs_uses_a_readable_unit_per_scale() {
        assert_eq!(humanize_secs(45), "45s");
        assert_eq!(humanize_secs(300), "5m");
        assert_eq!(humanize_secs(7_200), "2h");
        assert_eq!(humanize_secs(3 * 86_400), "3d");
    }

    #[test]
    fn pair_response_parses_empty_allow_and_renders_noop() {
        let resp: PairResponse =
            serde_json::from_str(r#"{"decision":"allow","warnings":[]}"#).unwrap();
        assert_eq!(resp.decision.as_deref(), Some("allow"));
        assert!(resp.warnings.is_empty());
        assert_eq!(
            format_pair_warnings(&resp),
            "No Pair warnings for this context."
        );
    }

    #[cfg(unix)]
    fn fake_agent_relay(script_body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-relay");
        fs::write(&path, format!("#!/bin/sh\n{script_body}\n")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        (dir, path)
    }

    fn one_shot_login_server(
        expected_agent_relay_token: &'static str,
        expected_mode: Option<&'static str>,
    ) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut content_length = 0usize;
            let mut first_line = String::new();
            reader.read_line(&mut first_line).unwrap();
            assert!(first_line.starts_with("POST /v1/cli/login "));
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let trimmed = line.trim_end();
                if trimmed.is_empty() {
                    break;
                }
                if let Some((name, value)) = trimmed.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        content_length = value.trim().parse().unwrap();
                    }
                }
            }
            let mut body = vec![0; content_length];
            reader.read_exact(&mut body).unwrap();
            let body = String::from_utf8(body).unwrap();
            let payload: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(payload["agentRelayToken"], expected_agent_relay_token);
            if let Some(mode) = expected_mode {
                assert_eq!(payload["mode"], mode);
            } else {
                assert!(payload.get("mode").is_none());
            }
            let response_body = r#"{"accessToken":"rth_at_abc","refreshToken":"rth_rt_def","orgId":"org_dev","workspaceId":"ws_dev","tokenType":"Bearer"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        (format!("http://{addr}"), handle)
    }

    #[cfg(unix)]
    #[test]
    fn login_via_cloud_exchanges_agent_relay_session() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (base_url, server) = one_shot_login_server("relay_at_abc", Some("sync"));
        // Models the real `agent-relay cloud session --json` contract: the access token comes
        // back MASKED unless `--reveal-token` is passed. If ai-hist ever drops that flag it gets
        // `relay_at_MASKED`, the login server below rejects the payload, and this test fails —
        // which is exactly the production failure it stands in for.
        let (_dir, bin) = fake_agent_relay(
            r#"case " $* " in
  *" --reveal-token "*)
    echo '{"apiUrl":"https://agentrelay.com/cloud","accessToken":"relay_at_abc","accessTokenExpiresAt":"2999-01-01T00:00:00.000Z"}'
    exit 0
    ;;
esac
if [ "$1 $2 $3" = "cloud session --json" ]; then
  echo '{"apiUrl":"https://agentrelay.com/cloud","accessToken":"relay_at_MASKED","accessTokenExpiresAt":"2999-01-01T00:00:00.000Z"}'
  exit 0
fi
echo "unexpected args: $*" 1>&2
exit 42"#,
        );
        let _agent_relay_bin = EnvVarGuard::set("AGENT_RELAY_BIN", bin.as_os_str());
        let _cloud_token = EnvVarGuard::remove("CLOUD_API_ACCESS_TOKEN");
        let _allow_dev_base_url =
            EnvVarGuard::set("RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL", "1");
        let auth = login_via_cloud(&base_url, "sync", None, "test-label").unwrap();
        server.join().unwrap();

        assert_eq!(auth.base_url, base_url);
        assert_eq!(auth.access_token, "rth_at_abc");
        assert_eq!(auth.refresh_token.as_deref(), Some("rth_rt_def"));
        assert_eq!(auth.org_id.as_deref(), Some("org_dev"));
        assert_eq!(auth.workspace_id.as_deref(), Some("ws_dev"));
    }

    /// Guards the exact defect that made `ai-hist login` fail fleet-wide: the session lookup must
    /// ask for the UNMASKED token. `agent-relay cloud session --json` masks it by default, and the
    /// masked stub is a plausible-looking `cld_at_…` string that only fails later, at Cloud
    /// `whoami`, as an opaque 401.
    #[cfg(unix)]
    #[test]
    fn cloud_session_invocation_requests_unmasked_token() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let log_dir = tempfile::tempdir().unwrap();
        let argv_log = log_dir.path().join("argv.log");
        // Pass the capture path via env and expand it quoted — a temp dir containing spaces or
        // shell metacharacters would otherwise break the fixture rather than the assertion.
        let _argv_log_env = EnvVarGuard::set("AI_HIST_TEST_ARGV_LOG", argv_log.as_os_str());
        let (_dir, bin) = fake_agent_relay(
            r#"printf '%s\n' "$@" > "$AI_HIST_TEST_ARGV_LOG"
echo '{"apiUrl":"https://agentrelay.com/cloud","accessToken":"relay_at_abc","accessTokenExpiresAt":"2999-01-01T00:00:00.000Z"}'
exit 0"#,
        );
        let _cloud_token = EnvVarGuard::remove("CLOUD_API_ACCESS_TOKEN");

        read_agent_relay_session(bin.to_str().unwrap()).unwrap();

        let argv = fs::read_to_string(&argv_log).unwrap();
        assert!(
            argv.contains("--reveal-token"),
            "ai-hist must request the unmasked token; without `--reveal-token` the CLI forwards a \
             masked stub and login fails with `invalid Agent Relay Cloud token`. argv was: {argv}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn cloud_session_failure_surfaces_stderr_never_stdout() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Failing exit: stdout carries a (would-be) secret; stderr carries the user-facing reason.
        let (_dir, bin) = fake_agent_relay(
            "echo 'rth_at_LEAKED_TOKEN_SHOULD_NOT_SURFACE'; echo 'not logged in: run agent-relay login' 1>&2; exit 1",
        );
        let err = read_agent_relay_session(bin.to_str().unwrap())
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("not logged in"),
            "stderr should surface: {err}"
        );
        // The bug we guard against: stdout (token-bearing) must NEVER appear in the error.
        assert!(
            !err.contains("rth_at_LEAKED_TOKEN_SHOULD_NOT_SURFACE"),
            "stdout leaked into error: {err}"
        );
    }

    #[test]
    fn login_via_cloud_rejects_invalid_mode() {
        let err = login_via_cloud("https://history.agentrelay.com", "admin", None, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid --mode"), "{err}");
    }

    #[test]
    fn login_via_cloud_rejects_workspace_without_mutating_global_agent_relay_state() {
        let err = login_via_cloud(
            "https://history.agentrelay.com",
            "sync",
            Some("workspace-a"),
            "test",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("not supported for Cloud login"), "{err}");
        assert!(err.contains("non-mutating"), "{err}");
    }

    #[test]
    fn login_via_cloud_rejects_non_default_base_url_without_explicit_dev_opt_in() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _allow = EnvVarGuard::remove("RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL");
        let err = login_via_cloud("http://localhost:8787", "sync", None, "test")
            .unwrap_err()
            .to_string();
        assert!(err.contains("refusing to send"), "{err}");
    }

    fn http_auth(base_url: &str) -> StoredAuth {
        StoredAuth {
            base_url: base_url.to_string(),
            access_token: "rth_at_secret".into(),
            ..Default::default()
        }
    }

    /// The token must never leave the machine in cleartext, from *any* of the calls that
    /// carry it — not just the newest one. `push` runs on a 300s timer, so it is the path
    /// that would leak most often.
    #[test]
    fn authenticated_calls_refuse_a_cleartext_base_url() {
        // `.invalid` is reserved and can never resolve (RFC 2606), so if this guard is ever
        // removed the test fails on the assertion rather than by putting the token on a wire.
        let auth = http_auth("http://history.relayhistory.invalid");

        let coverage = fleet_coverage(&auth, &CoverageQuery::default()).unwrap_err();
        let ingest = UreqIngestor
            .ingest(
                &auth,
                &IngestRequest {
                    machine: MachineIdentity::default(),
                    batch_id: "b_test".into(),
                    cursors: None,
                    records: Vec::new(),
                },
            )
            .unwrap_err();
        let pair = pair_check(&auth, &PairContext::default(), 5).unwrap_err();

        for err in [coverage, ingest, pair] {
            let msg = err.to_string();
            assert!(msg.contains("refusing to send"), "{msg}");
            // The failure must not itself disclose what it was protecting.
            assert!(!msg.contains("rth_at_secret"), "{msg}");
        }
    }

    /// `wrangler dev` is the documented local endpoint and never puts bytes on a network,
    /// so it stays usable — no opt-in flag, which would be friction bought with no security.
    #[test]
    fn loopback_stays_usable_for_local_development() {
        for url in [
            "http://localhost:8787",
            "http://127.0.0.1:8787",
            "http://[::1]:8787",
            "http://LocalHost:8787/",
        ] {
            assert!(
                require_secure_transport(url).is_ok(),
                "loopback dev endpoint rejected: {url}"
            );
        }
        assert!(require_secure_transport("https://history.agentrelay.com").is_ok());
    }

    /// A hostname that merely reads as loopback is a remote host. Userinfo is the classic
    /// way to dress one up.
    #[test]
    fn loopback_exemption_cannot_be_spoofed_by_a_lookalike_host() {
        for url in [
            "http://127.0.0.1.evil.com/",
            "http://localhost.evil.com:8787",
            "http://127.0.0.1@evil.com/",
            "http://user@localhost@evil.com/",
            "http://evil.com/?h=localhost",
            "http://evil.com/#127.0.0.1",
            "http://[::1]@evil.com/",
            "ftp://localhost:8787",
            "history.agentrelay.com",
        ] {
            assert!(
                require_secure_transport(url).is_err(),
                "non-loopback endpoint accepted: {url}"
            );
        }
    }

    #[test]
    fn default_base_url_honors_env_override() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _relayhistory_base_url =
            EnvVarGuard::set("RELAYHISTORY_BASE_URL", "http://localhost:8787/");
        assert_eq!(default_base_url(), "http://localhost:8787");
    }

    #[test]
    fn default_base_url_falls_through_empty_primary_env() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _relayhistory_base_url = EnvVarGuard::set("RELAYHISTORY_BASE_URL", "///");
        let _ai_hist_base_url = EnvVarGuard::set("AI_HIST_BASE_URL", "http://localhost:8787/");
        assert_eq!(default_base_url(), "http://localhost:8787");
    }
    // ----- conversation turns on the push path -----

    fn turns_test_auth() -> StoredAuth {
        StoredAuth {
            base_url: "http://localhost:8787".into(),
            access_token: "rth_at_test".into(),
            ..Default::default()
        }
    }

    /// Records every turns request and can be told to fail for one session, so the tests
    /// can check both the happy path and what a partial failure does to the watermark.
    struct TurnsRecordingIngestor {
        published: RefCell<Vec<(String, usize)>>,
        fail_session: Option<String>,
    }

    impl TurnsRecordingIngestor {
        fn new() -> Self {
            Self {
                published: RefCell::new(Vec::new()),
                fail_session: None,
            }
        }
        fn failing(session: &str) -> Self {
            Self {
                published: RefCell::new(Vec::new()),
                fail_session: Some(session.to_string()),
            }
        }
    }

    impl Ingestor for TurnsRecordingIngestor {
        fn ingest(&self, _auth: &StoredAuth, req: &IngestRequest) -> Result<IngestResponse> {
            Ok(IngestResponse {
                batch_id: req.batch_id.clone(),
                received: req.records.len() as u64,
                accepted: req.records.len() as u64,
                cursors: None,
            })
        }

        fn publish_turns(
            &self,
            _auth: &StoredAuth,
            session_id: &str,
            turns: &[ConversationTurn],
        ) -> Result<u64> {
            if self.fail_session.as_deref() == Some(session_id) {
                anyhow::bail!("turns failed: HTTP 500");
            }
            self.published
                .borrow_mut()
                .push((session_id.to_string(), turns.len()));
            Ok(turns.len() as u64)
        }
    }

    fn add_session_event(conn: &Connection, session: &str, ts: i64, role: &str, text: &str) {
        conn.execute(
            "INSERT INTO session_events (source, session_id, ts_ms, role, kind, text, model, event_uid)
             VALUES ('claude', ?1, ?2, ?3, 'text', ?4, 'claude-opus-5', ?5)",
            rusqlite::params![session, ts, role, text, format!("{session}-{ts}-{role}")],
        )
        .unwrap();
    }

    /// The transcript backlog must not be gated on new prompts. Most machines reach
    /// "prompts fully synced" long before their transcript is published, and a run that
    /// reported `sent: 0` while silently skipping turns would look exactly like a
    /// machine with nothing to do.
    #[test]
    fn publishes_turns_even_when_there_are_no_new_convergence_records() {
        with_temp_home(|| {
            let conn = mem();
            add_session_event(&conn, "s1", 1_000, "user", "why does it fail?");
            add_session_event(&conn, "s1", 2_000, "assistant", "the regex is unbounded");

            let client = TurnsRecordingIngestor::new();
            let auth = turns_test_auth();

            let report = push(
                &conn,
                &client,
                &auth,
                &MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                &SyncCursor::default(),
                500,
                &HashSet::new(),
            )
            .unwrap();

            assert_eq!(report.sent, 0, "no history rows exist");
            assert_eq!(report.turns_sent, 2, "but the transcript must still go");
            assert_eq!(report.turns_sessions, 1);
            assert_eq!(client.published.borrow().len(), 1);
            assert!(report.cursor.session_event_id > 0);
        });
    }

    #[test]
    fn publishes_both_sides_of_the_conversation_not_just_prompts() {
        with_temp_home(|| {
            let conn = mem();
            add(&conn, "why does it fail?", 1_000);
            add_session_event(&conn, "s1", 1_000, "user", "why does it fail?");
            add_session_event(&conn, "s1", 2_000, "assistant", "the regex is unbounded");

            let client = TurnsRecordingIngestor::new();
            let report = push(
                &conn,
                &client,
                &turns_test_auth(),
                &MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                &SyncCursor::default(),
                500,
                &HashSet::new(),
            )
            .unwrap();

            assert!(report.sent > 0, "the prompt still goes through convergence");
            assert_eq!(report.turns_sent, 2, "and the assistant reply goes too");
        });
    }

    /// A half-published transcript must not advance the watermark. If it did, the session
    /// would stay permanently truncated and every later run would report a clean
    /// `turns_sent: 0` — the failure would be indistinguishable from being up to date.
    #[test]
    fn a_failed_transcript_does_not_advance_the_watermark() {
        with_temp_home(|| {
            let conn = mem();
            add_session_event(&conn, "s1", 1_000, "user", "hello");

            let client = TurnsRecordingIngestor::failing("s1");
            let report = push(
                &conn,
                &client,
                &turns_test_auth(),
                &MachineIdentity {
                    id: "m1".into(),
                    ..Default::default()
                },
                &SyncCursor::default(),
                500,
                &HashSet::new(),
            )
            .unwrap();

            assert_eq!(report.turns_sent, 0);
            assert_eq!(
                report.cursor.session_event_id, 0,
                "the watermark must stay put so the next run retries",
            );
        });
    }

    /// A convergence-mapper replay must not restart the transcript stream. They are
    /// separate backlogs; conflating them would re-send every turn on every mapper bump.
    #[test]
    fn a_capture_version_replay_leaves_the_turns_watermark_alone() {
        let old = SyncCursor {
            capture_version: 0,
            history_id: 5_000,
            session_event_id: 4_242,
            ..Default::default()
        };
        let upgraded = old.merge_max(&SyncCursor {
            capture_version: SyncCursor::CAPTURE_VERSION,
            history_id: 0,
            session_event_id: 4_242,
            ..Default::default()
        });

        assert_eq!(upgraded.history_id, 0, "convergence rewinds");
        assert_eq!(
            upgraded.session_event_id, 4_242,
            "the transcript watermark must survive a convergence replay",
        );
    }

    #[test]
    fn encodes_a_session_id_that_would_otherwise_change_the_url() {
        assert_eq!(encode_path_segment("abc-123_x.y~z"), "abc-123_x.y~z");
        assert_eq!(encode_path_segment("a/b"), "a%2Fb");
        assert_eq!(encode_path_segment("a?b=c"), "a%3Fb%3Dc");
        assert_eq!(encode_path_segment("a b"), "a%20b");
    }
}
