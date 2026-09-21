//! Explicit compatibility hooks for existing Agent Relay integrations.
use crate::{cloud, convergence::MachineIdentity};
use anyhow::Result;
use std::collections::HashSet;
/// Result of an in-process [`sync_and_push`] run.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPushOutcome {
    pub sent: u64,
    pub accepted: u64,
    /// `false` when there's no stored relayhistory auth yet (treated as a no-op
    /// rather than an error, for background callers).
    pub authenticated: bool,
    /// `true` when another process owned the scan lock. Already-indexed rows are still pushed.
    pub sync_skipped: bool,
}

pub fn sync_and_push() -> Result<SyncPushOutcome> {
    let db_path = ai_hist::default_db_path();
    let (conn, sync_skipped) = ai_hist::prepare_local_sync_snapshot(&db_path)?;

    // The in-process runtime has no CLI argument channel. Keep it pinned to the normal Cloud
    // origin rather than following whichever stage happened to be logged into most recently.
    let default_base_url = cloud::default_base_url();
    let auth = match cloud::load_auth(Some(&default_base_url))? {
        Some(auth) => auth,
        None => {
            return Ok(SyncPushOutcome {
                sent: 0,
                accepted: 0,
                authenticated: false,
                sync_skipped,
            })
        }
    };
    let machine = MachineIdentity {
        id: cloud::machine_id()?,
        hostname: cloud::machine_hostname(),
        os: Some(std::env::consts::OS.to_string()),
        cli_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        ..Default::default()
    };
    let cursor = cloud::load_cursor(&auth.base_url)?;
    let report = cloud::push(
        &conn,
        &cloud::UreqIngestor,
        &auth,
        &machine,
        &cursor,
        500,
        &HashSet::new(),
    )?;
    Ok(SyncPushOutcome {
        sent: report.sent as u64,
        accepted: report.accepted,
        authenticated: true,
        sync_skipped,
    })
}
