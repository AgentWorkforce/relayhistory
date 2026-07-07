//! Native (napi) binding for driving ai-hist in-process — no CLI shell-out.
//!
//! Exposes `syncAndPush()` to Node. The Agent Relay runtime calls this from its
//! Reflex capture loop so history is synced + pushed inside the process.
#![deny(clippy::all)]

use napi_derive::napi;

/// Result of one in-process sync+push.
#[napi(object)]
pub struct SyncPushResult {
    pub sent: u32,
    pub accepted: u32,
    /// `false` when there's no stored relayhistory auth yet (a no-op, not an error).
    pub authenticated: bool,
}

/// Sync local agent history into the ai-hist DB, then push new records to
/// relayhistory-cloud. The blocking work (file/SQLite/HTTP) runs on a worker
/// thread so the Node event loop is never blocked.
#[napi]
pub async fn sync_and_push() -> napi::Result<SyncPushResult> {
    let outcome = napi::tokio::task::spawn_blocking(ai_hist_cli::sync_and_push)
        .await
        .map_err(|e| napi::Error::from_reason(format!("worker thread panicked: {e}")))?
        .map_err(|e| napi::Error::from_reason(format!("{e:#}")))?;
    Ok(SyncPushResult {
        sent: outcome.sent as u32,
        accepted: outcome.accepted as u32,
        authenticated: outcome.authenticated,
    })
}
