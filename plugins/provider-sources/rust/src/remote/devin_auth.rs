//! `devin-cloud` configuration: the Devin API coordinates resolved from the
//! Devin CLI's own files plus explicit environment overrides. The bearer is
//! never logged or returned.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::Value;

use super::claude_web::require_https_or_loopback;
use super::devin_cloud::{DevinCloudProvider, UreqDevinTransport};

// ---------------------------------------------------------------------------
// configuration — Devin CLI files plus explicit environment overrides
// ---------------------------------------------------------------------------

/// The Devin API coordinates resolved from the Devin CLI's own files and
/// explicit environment overrides. The bearer is never logged or returned.
pub(super) struct DevinAuth {
    pub(super) bearer: String,
    pub(super) org_id: String,
    pub(super) base_url: String,
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// The Devin credential file: `RELAYHISTORY_DEVIN_CREDENTIALS`, else the
/// CLI's `credentials.toml` under its data directory.
pub(super) fn devin_credentials_path(home: &Path) -> PathBuf {
    if let Some(path) = std::env::var_os("RELAYHISTORY_DEVIN_CREDENTIALS") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    devin_data_home(home).join("credentials.toml")
}

/// The Devin CLI config: `RELAYHISTORY_DEVIN_CONFIG`, else `config.json`
/// under its config directory. Holds `devin.org_id`.
pub(super) fn devin_config_path(home: &Path) -> PathBuf {
    if let Some(path) = std::env::var_os("RELAYHISTORY_DEVIN_CONFIG") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    devin_config_home(home).join("config.json")
}

fn devin_data_home(home: &Path) -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share"))
        .join("devin")
}

fn devin_config_home(home: &Path) -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".config"))
        .join("devin")
}

/// One `key = "value"` line's value from Devin's flat `credentials.toml` —
/// deliberately a line scan, not a TOML dependency for a two-key file.
pub(super) fn toml_value(raw: &str, key: &str) -> Option<String> {
    for line in raw.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        if name.trim() != key {
            continue;
        }
        if let Some(unquoted) = value
            .trim()
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            return Some(unquoted.to_string());
        }
    }
    None
}

/// `devin.org_id` from the Devin CLI's `config.json`, when present.
pub(super) fn devin_org_id(config_path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(config_path).ok()?;
    let value: Value = serde_json::from_str(&raw).ok()?;
    value
        .pointer("/devin/org_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn read_devin_credentials(path: &Path) -> (Option<String>, Option<String>) {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return (None, None);
    };
    (
        toml_value(&raw, "windsurf_api_key"),
        toml_value(&raw, "devin_api_url"),
    )
}

pub(super) fn devin_auth(credentials_path: &Path, config_path: &Path) -> Result<DevinAuth> {
    let (credential_key, credential_url) = read_devin_credentials(credentials_path);
    let bearer = env_non_empty("RELAYHISTORY_DEVIN_API_KEY")
        .or(credential_key)
        .context("CONNECTOR_NOT_CONFIGURED: Devin API key unavailable; sign in with the Devin CLI or set RELAYHISTORY_DEVIN_API_KEY")?;
    let org_id = env_non_empty("RELAYHISTORY_DEVIN_ORG_ID")
        .or_else(|| devin_org_id(config_path))
        .context("CONNECTOR_NOT_CONFIGURED: Devin organization id unavailable; set devin.org_id in the Devin CLI config or RELAYHISTORY_DEVIN_ORG_ID")?;
    let base_url = env_non_empty("RELAYHISTORY_DEVIN_API_BASE_URL")
        .or(credential_url)
        .unwrap_or_else(|| "https://api.devin.ai".to_string());
    let base_url = base_url.trim_end_matches('/').to_string();
    require_https_or_loopback(&base_url)
        .context("CONNECTOR_NOT_CONFIGURED: invalid Devin API base URL")?;
    // The org id is interpolated into the request path; a delimiter would
    // retarget an authenticated request onto a different route.
    anyhow::ensure!(
        org_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "CONNECTOR_NOT_CONFIGURED: Devin organization id contains characters outside [A-Za-z0-9_-]"
    );
    Ok(DevinAuth {
        bearer,
        org_id,
        base_url,
    })
}

/// Whether a credentials file and an organization id both resolve under this
/// home — the status probe's notion of "configured".
pub(super) fn devin_auth_ready(home: &Path) -> bool {
    let credentials = devin_credentials_path(home);
    let credential_key = read_devin_credentials(&credentials).0;
    let has_key = env_non_empty("RELAYHISTORY_DEVIN_API_KEY").is_some() || credential_key.is_some();
    let has_org = env_non_empty("RELAYHISTORY_DEVIN_ORG_ID").is_some()
        || devin_org_id(&devin_config_path(home)).is_some();
    has_key && has_org
}

/// The connector's status-probe answer.
pub(super) fn devin_status(home: &Path) -> (bool, String) {
    let path = devin_credentials_path(home);
    if devin_auth_ready(home) {
        (true, format!("Devin CLI sign-in at {}", path.display()))
    } else {
        (
            false,
            format!(
                "no Devin API key at {} (or RELAYHISTORY_DEVIN_API_KEY) and no org id (or RELAYHISTORY_DEVIN_ORG_ID)",
                path.display()
            ),
        )
    }
}

/// Provider construction shared by `provider()` and bulk selection.
pub(super) fn devin_provider(home: &Path, limit: Option<usize>) -> DevinCloudProvider {
    DevinCloudProvider::new(
        devin_credentials_path(home),
        devin_config_path(home),
        Box::new(UreqDevinTransport),
        limit,
    )
}

/// `Some(provider)` only when the connector is configured on this machine.
pub(super) fn devin_provider_at(home: &Path, limit: Option<usize>) -> Option<DevinCloudProvider> {
    devin_auth_ready(home).then(|| devin_provider(home, limit))
}
