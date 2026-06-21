//! WS-9 cloud-sync increment 2b: the client transport for pushing the local recall store
//! to relayhistory-cloud (Agent Relay Loop).
//!
//! This is the **binding layer** — it does the network I/O the WASM-bound `ai-hist-core`
//! deliberately avoids. It wires `ai_hist_core::outbox::build_outbox_batch` (pure batch
//! building) to `POST /v1/ingest` with `rth_at_` bearer auth, persists the single cursor
//! store, and advances it to the server-confirmed watermark.
//!
//! Token bootstrap: `/v1/cli/login` (RelayAuth JWT → `rth_at_`/`rth_rt_`) for real use, or
//! `/v1/admin/mint` (dev-only, `ADMIN_MINT_SECRET`) for local `wrangler dev` iteration.
//!
//! The HTTP call is behind the [`Ingestor`] trait so the push orchestration (batch build,
//! cursor advance, idempotent batchId, no-op-on-empty) is unit-testable without a server.

use ai_hist_core::convergence::{IngestRequest, IngestResponse, MachineIdentity};
use ai_hist_core::outbox::{build_outbox_batch, SyncCursor};
use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

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

fn auth_path() -> PathBuf {
    config_dir().join("auth.json")
}
fn cursor_path() -> PathBuf {
    config_dir().join("cursor.json")
}
fn machine_path() -> PathBuf {
    config_dir().join("machine-id")
}

fn write_private(path: &std::path::Path, body: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, body)?;
    // best-effort 0600 on unix (token/secret hygiene)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

pub fn load_auth() -> Result<Option<StoredAuth>> {
    let path = auth_path();
    if !path.exists() {
        return Ok(None);
    }
    let body = fs::read_to_string(&path)?;
    Ok(Some(
        serde_json::from_str(&body).context("parsing stored auth.json")?,
    ))
}

pub fn save_auth(auth: &StoredAuth) -> Result<()> {
    write_private(&auth_path(), &serde_json::to_string_pretty(auth)?)
}

pub fn load_cursor() -> Result<SyncCursor> {
    let path = cursor_path();
    if !path.exists() {
        return Ok(SyncCursor::default());
    }
    let body = fs::read_to_string(&path)?;
    serde_json::from_str(&body).context("parsing cursor.json")
}

pub fn save_cursor(cursor: &SyncCursor) -> Result<()> {
    write_private(&cursor_path(), &serde_json::to_string_pretty(cursor)?)
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
    let id = format!("m_{}", ai_hist_core::prompt_hash(&format!("{host}:{nanos}")));
    write_private(&path, &id)?;
    Ok(id)
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown-host".to_string())
}

/// Deterministic, retry-safe batch id: a hash of the batch's contents. Re-pushing the same
/// (machine, cursor span, record count) batch reuses the id, so the server's
/// `(orgId, machineId, batchId)` dedup makes a retry a no-op.
pub fn batch_id(machine: &str, from: &SyncCursor, to: &SyncCursor, count: usize) -> String {
    format!(
        "b_{}",
        ai_hist_core::prompt_hash(&format!(
            "{machine}:{}:{}:{}:{}:{count}",
            from.history_id, from.trajectory_rowid, to.history_id, to.trajectory_rowid
        ))
    )
}

/// The HTTP side of `/v1/ingest`, abstracted so the push orchestration is testable.
pub trait Ingestor {
    fn ingest(&self, auth: &StoredAuth, req: &IngestRequest) -> Result<IngestResponse>;
}

/// Result of a `push` run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushReport {
    pub sent: usize,
    pub accepted: u64,
    pub cursor: SyncCursor,
    pub batch_id: Option<String>,
}

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
    let batch = build_outbox_batch(conn, cursor, limit, incognito)?;
    if batch.records.is_empty() {
        return Ok(PushReport {
            sent: 0,
            accepted: 0,
            cursor: cursor.clone(),
            batch_id: None,
        });
    }
    let bid = batch_id(&machine.id, cursor, &batch.cursor, batch.records.len());
    let req = IngestRequest {
        machine: machine.clone(),
        batch_id: bid.clone(),
        cursors: Some(serde_json::json!({
            "history_id": batch.cursor.history_id,
            "trajectory_rowid": batch.cursor.trajectory_rowid,
        })),
        records: batch.records,
    };
    let resp = client.ingest(auth, &req).context("POST /v1/ingest")?;
    // advance the cursor only after the server accepts the batch (durable outbox)
    save_cursor(&batch.cursor)?;
    Ok(PushReport {
        sent: req.records.len(),
        accepted: resp.accepted,
        cursor: batch.cursor,
        batch_id: Some(bid),
    })
}

// ----- ureq-backed live transport -----

/// Live `Ingestor` over `ureq` (blocking HTTP — no async runtime, never compiled into the
/// WASM core).
pub struct UreqIngestor;

impl Ingestor for UreqIngestor {
    fn ingest(&self, auth: &StoredAuth, req: &IngestRequest) -> Result<IngestResponse> {
        let url = format!("{}/v1/ingest", auth.base_url.trim_end_matches('/'));
        let resp = ureq::post(&url)
            .set("Authorization", &format!("Bearer {}", auth.access_token))
            .set("Content-Type", "application/json")
            .send_json(serde_json::to_value(req)?);
        match resp {
            Ok(r) => Ok(r.into_json::<IngestResponse>()?),
            Err(ureq::Error::Status(code, r)) => {
                let body = r.into_string().unwrap_or_default();
                anyhow::bail!("ingest failed: HTTP {code}: {body}")
            }
            Err(e) => Err(e.into()),
        }
    }
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
        refresh_token: v.get("refreshToken").and_then(|x| x.as_str()).map(String::from),
        org_id: Some(org_id.to_string()),
        workspace_id: workspace_id.map(String::from),
    })
}

/// `POST /v1/cli/login` (RelayAuth JWT → `rth_at_`/`rth_rt_`) — the real-use bootstrap.
pub fn login(base_url: &str, agent_relay_token: &str, label: &str) -> Result<StoredAuth> {
    let url = format!("{}/v1/cli/login", base_url.trim_end_matches('/'));
    let resp = ureq::post(&url)
        .set("Content-Type", "application/json")
        .send_json(serde_json::json!({ "agentRelayToken": agent_relay_token, "label": label }))
        .map_err(map_http_err)?;
    let v: serde_json::Value = resp.into_json()?;
    Ok(StoredAuth {
        base_url: base_url.trim_end_matches('/').to_string(),
        access_token: field(&v, "accessToken")?,
        refresh_token: v.get("refreshToken").and_then(|x| x.as_str()).map(String::from),
        org_id: v.get("orgId").and_then(|x| x.as_str()).map(String::from),
        workspace_id: v.get("workspaceId").and_then(|x| x.as_str()).map(String::from),
    })
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
        insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some("s1".into()),
                project: None,
                prompt: prompt.into(),
                prompt_hash: Some(ai_hist_core::prompt_hash(prompt)),
                timestamp_ms: ts,
            },
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

    // RELAYHISTORY_HOME is process-global; serialize env-home tests so cargo's parallel
    // runner can't clobber it across tests.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_temp_home<T>(f: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("RELAYHISTORY_HOME", dir.path());
        let out = f();
        std::env::remove_var("RELAYHISTORY_HOME");
        out
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
            assert_eq!(load_cursor().unwrap().history_id, 2);
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
            assert_eq!(load_auth().unwrap().unwrap(), auth);
            let c = SyncCursor {
                history_id: 9,
                trajectory_rowid: 4,
            };
            save_cursor(&c).unwrap();
            assert_eq!(load_cursor().unwrap(), c);
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
}
