//! Explicit operator retention: preview only until --apply is supplied.
use ai_hist::retention::{
    read_retention_generation, read_retention_policy, RetentionOptions, RetentionPolicy,
};
use ai_hist::{SessionStore, StoreOptions};
use anyhow::Result;
use serde_json::{json, Value};
use std::path::Path;

pub(super) fn run(
    path: &Path,
    days: Option<u32>,
    apply: bool,
    allow_unuploaded: bool,
    max_rows: u32,
    json_output: bool,
) -> Result<()> {
    anyhow::ensure!((1..=1024).contains(&max_rows), "max-rows must be 1..=1024");
    anyhow::ensure!(
        !allow_unuploaded || apply,
        "--allow-unuploaded requires --apply"
    );
    let stored = if path.exists() {
        read_retention_policy(path)?
    } else {
        RetentionPolicy::default()
    };
    let stored_days = stored.days.map_or(0, std::num::NonZeroU32::get);
    let effective_days = days.unwrap_or(stored_days);
    let (generation, report) = if apply {
        let store = SessionStore::open(StoreOptions::default().db_path(path))?;
        if days.is_some() {
            store.set_retention_policy(RetentionPolicy::from_days(effective_days))?;
        }
        let report = store.prune_expired(RetentionOptions {
            max_rows,
            allow_unuploaded,
            ..Default::default()
        })?;
        (
            store.retention_generation()?,
            json!({"rows":report.rows,"sessions":report.sessions,"protected_sessions":report.protected_sessions,
            "awaiting_upload_ack":report.awaiting_upload_ack,"busy":report.busy,"deferred":report.deferred,"cycle_complete":report.cycle_complete}),
        )
    } else {
        (
            if path.exists() {
                read_retention_generation(path)?
            } else {
                0
            },
            Value::Null,
        )
    };
    if json_output {
        println!(
            "{}",
            json!({"dry_run":!apply,"preview":"policy_only","stored_days":stored_days,"effective_days":effective_days,
            "max_rows":max_rows,"allow_unuploaded":allow_unuploaded,"generation":generation,"report":report})
        );
    } else if apply {
        println!("Retention batch: {report}");
    } else {
        println!("Policy preview only: {effective_days} days (0 means forever). No changes made. Use --apply to save policy and run one bounded batch.");
    }
    Ok(())
}
