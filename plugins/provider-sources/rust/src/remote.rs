//! Optional provider-native remote transports. No RelayHistory authentication is read.
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Mutex};
use std::time::{Duration, Instant};

use ai_hist::{SessionLocation, SOURCE_CHOICES};
use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;

use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};

/// Connector name for the claude.ai/code web-session lister.
pub const CLAUDE_WEB_CONNECTOR: &str = "claude-web";
/// Connector name for the Codex cloud task lister.
pub const CODEX_CLOUD_CONNECTOR: &str = "codex-cloud";

/// Most listing pages one enumeration may fetch, whatever the caller asked.
const MAX_LIST_PAGES: usize = 100;
/// Bytes of remote evidence one acquisition may retain. One extra byte is
/// read only to detect the limit.
const MAX_REMOTE_EVIDENCE_BYTES: usize = 16 * 1024 * 1024;

pub use crate::sources::AcquiredEvidence as RemoteSessionEvidence;

mod claude_web;
mod codex_diff;

use codex_diff::acquire_codex_remote_session;
#[cfg(test)]
use codex_diff::{
    acquire_codex_remote_session_with_command, acquire_codex_remote_session_with_command_timeout,
};

#[cfg(test)]
use claude_web::{
    acquire_claude_remote_session_at, acquire_remote_session_at, map_claude_web_session,
    require_https_or_loopback,
};
use claude_web::{claude_api_base_url, excerpt_one_line, UreqClaudeTransport};
pub use claude_web::{ClaudeHttpResponse, ClaudeSessionsTransport, ClaudeWebProvider};

/// Whether one remote connector can run on this machine, and why not when it
/// cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteConnectorStatus {
    /// Connector name (`claude-web`, `codex-cloud`, `cloud`).
    pub connector: &'static str,
    /// The upstream source, or `"*"` for a cross-source connector.
    pub source: &'static str,
    /// `true` when the provider CLI's stored sign-in was found.
    pub configured: bool,
    /// Human-readable detail: the credential looked for, or where it was found.
    pub detail: String,
}

fn claude_credentials_path(home: &Path) -> PathBuf {
    if let Some(path) = std::env::var_os("RELAYHISTORY_CLAUDE_CREDENTIALS") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    home.join(".claude/.credentials.json")
}

fn codex_auth_path(home: &Path) -> PathBuf {
    home.join(".codex/auth.json")
}

/// Explicit built-in source connector allowlist. Omission retains provider CLI
/// connectors; an empty list disables remote acquisition. Commercial credentials
/// never add a connector. Instance-specific registration follows the observation
/// migration; these built-ins currently represent one default instance each.
#[derive(Debug, Clone)]
pub struct SourceConnectorSelection {
    ids: Vec<String>,
}

impl Default for SourceConnectorSelection {
    fn default() -> Self {
        Self {
            ids: vec![CLAUDE_WEB_CONNECTOR.into(), CODEX_CLOUD_CONNECTOR.into()],
        }
    }
}

impl SourceConnectorSelection {
    pub fn new(ids: Vec<String>) -> Result<Self> {
        for id in &ids {
            anyhow::ensure!(
                [CLAUDE_WEB_CONNECTOR, CODEX_CLOUD_CONNECTOR,].contains(&id.as_str()),
                "invalid source connector '{id}'"
            );
        }
        let mut ids = ids;
        ids.sort();
        ids.dedup();
        Ok(Self { ids })
    }

    pub fn contains(&self, id: &str) -> bool {
        self.ids.iter().any(|selected| selected == id)
    }

    pub fn ids(&self) -> &[String] {
        &self.ids
    }
}

/// Provider-only availability; this operation never reads commercial auth.
pub fn remote_connector_statuses_at(home: &Path) -> Vec<RemoteConnectorStatus> {
    selected_remote_connector_statuses_at(home, &SourceConnectorSelection::default(), &[])
}

/// Probe only selected connectors applicable to the source filter. Selection is
/// evaluated before touching any credential store or commercial environment.
pub fn selected_remote_connector_statuses_at(
    home: &Path,
    selection: &SourceConnectorSelection,
    sources: &[String],
) -> Vec<RemoteConnectorStatus> {
    let mut statuses = Vec::new();
    for (connector, source) in [
        (CLAUDE_WEB_CONNECTOR, "claude"),
        (CODEX_CLOUD_CONNECTOR, "codex"),
    ] {
        if !selection.contains(connector)
            || (!sources.is_empty() && !sources.iter().any(|s| s == source))
        {
            continue;
        }
        let path = if connector == CLAUDE_WEB_CONNECTOR {
            claude_credentials_path(home)
        } else {
            codex_auth_path(home)
        };
        let configured = path.is_file();
        statuses.push(RemoteConnectorStatus {
            connector,
            source,
            configured,
            detail: if configured {
                format!("provider CLI login at {}", path.display())
            } else {
                format!(
                    "no provider CLI credentials at {} (sign in with the provider CLI)",
                    path.display()
                )
            },
        });
    }
    statuses
}

/// [`remote_connector_statuses_at`] under the process home directory.
pub fn remote_connector_statuses() -> Vec<RemoteConnectorStatus> {
    remote_connector_statuses_at(&crate::home_dir())
}

/// The error a remote-only acquisition request gets when nothing is configured.
///
/// The leading phrase is a compatibility contract: callers and tests match on
/// "no remote provider connectors are configured".
pub(crate) fn unconfigured_message(operation: &str, statuses: &[RemoteConnectorStatus]) -> String {
    let reasons = statuses
        .iter()
        .map(|status| format!("{}: {}", status.connector, status.detail))
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "no remote provider connectors are configured: remote session {operation} is not available ({reasons})"
    )
}

/// Reject a remote-only acquisition when no connector is configured, using the
/// process home directory. Cheap (a couple of `stat`s), so callers run it
/// before opening the ledger.
pub fn ensure_remote_connectors_configured(operation: &str) -> Result<()> {
    ensure_remote_connectors_configured_at(operation, &crate::home_dir())
}

/// [`ensure_remote_connectors_configured`] under an explicit home directory.
pub fn ensure_remote_connectors_configured_at(operation: &str, home: &Path) -> Result<()> {
    ensure_remote_connectors_configured_for_at(operation, home, &[])
}

/// Reject a remote-only acquisition that no configured connector can serve
/// once a source filter is applied, using the process home directory.
///
/// An empty `sources` filter means "every source". A filter that names only
/// sources without a remote connector (or whose connectors are not signed
/// in) is the same unsupported request, scoped down — callers classify both
/// as unsupported-operation, never as a runtime discovery failure.
pub fn ensure_remote_connectors_configured_for(operation: &str, sources: &[String]) -> Result<()> {
    ensure_remote_connectors_configured_for_at(operation, &crate::home_dir(), sources)
}

/// [`ensure_remote_connectors_configured_for`] under an explicit home directory.
pub fn ensure_remote_connectors_configured_for_at(
    operation: &str,
    home: &Path,
    sources: &[String],
) -> Result<()> {
    ensure_selected_remote_connectors_configured_for_at(
        operation,
        home,
        sources,
        &SourceConnectorSelection::default(),
    )
}

pub fn ensure_selected_remote_connectors_configured_for(
    operation: &str,
    sources: &[String],
    selection: &SourceConnectorSelection,
) -> Result<()> {
    ensure_selected_remote_connectors_configured_for_at(
        operation,
        &crate::home_dir(),
        sources,
        selection,
    )
}

pub fn ensure_selected_remote_connectors_configured_for_at(
    operation: &str,
    home: &Path,
    sources: &[String],
    selection: &SourceConnectorSelection,
) -> Result<()> {
    // A misspelled source is an invalid argument, not an unsupported remote
    // request — reject it with the engine's own invalid-source message before
    // classifying anything.
    for source in sources {
        anyhow::ensure!(
            SOURCE_CHOICES.contains(&source.as_str()),
            "invalid source '{source}' (choose from {})",
            SOURCE_CHOICES.join(", ")
        );
    }
    // Reject capabilities before probing credentials too: commercial recall
    // has no targeted hydration.
    let applicable = SourceConnectorSelection {
        ids: selection
            .ids
            .iter()
            .filter(|id| match operation {
                "sync" => true,
                "hydration" | "hydrate" => {
                    matches!(id.as_str(), CLAUDE_WEB_CONNECTOR | CODEX_CLOUD_CONNECTOR)
                }
                _ => true,
            })
            .cloned()
            .collect(),
    };
    let statuses = selected_remote_connector_statuses_at(home, &applicable, sources);
    anyhow::ensure!(
        !statuses.is_empty(),
        "remote session {operation} is not available for the requested source(s): no matching remote provider connectors exist"
    );
    anyhow::ensure!(
        statuses.iter().any(|status| status.configured),
        unconfigured_message(operation, &statuses)
    );
    Ok(())
}

/// Every configured remote connector, as shallow providers the discovery
/// engine can run beside the local adapters. Unconfigured connectors are
/// simply absent — for `all` scope that is the documented "runs whatever is
/// available" behaviour, and for `remote` scope the caller has already
/// rejected the empty set.
pub fn selected_remote_providers(
    home: &Path,
    limit: Option<usize>,
    selection: &SourceConnectorSelection,
    sources: &[String],
) -> Vec<Box<dyn ShallowSessionProvider>> {
    let applicable = |source: &str| sources.is_empty() || sources.iter().any(|s| s == source);
    let mut providers: Vec<Box<dyn ShallowSessionProvider>> = Vec::new();
    let claude_path = claude_credentials_path(home);
    if selection.contains(CLAUDE_WEB_CONNECTOR) && applicable("claude") && claude_path.is_file() {
        providers.push(Box::new(ClaudeWebProvider::new(
            claude_path,
            claude_api_base_url(),
            Box::new(UreqClaudeTransport),
            limit,
        )));
    }
    let codex_path = codex_auth_path(home);
    if selection.contains(CODEX_CLOUD_CONNECTOR) && applicable("codex") && codex_path.is_file() {
        providers.push(Box::new(CodexCloudProvider::new(
            Box::new(ExecCodexCli),
            limit,
        )));
    }
    providers
}

// ---------------------------------------------------------------------------
// shared plumbing
// ---------------------------------------------------------------------------

/// Rows one remote enumeration fetched, keyed by candidate locator, waiting
/// for the engine's `read_shallow` calls. The listing already carried every
/// field, so the "read" is a map lookup.
type FetchedRows = Mutex<BTreeMap<String, ShallowSession>>;

fn take_fetched(rows: &FetchedRows, locator: &str) -> Option<ShallowSession> {
    rows.lock()
        .expect("remote connector row cache")
        .get(locator)
        .cloned()
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

// ---------------------------------------------------------------------------
// codex-cloud
// ---------------------------------------------------------------------------

/// The Codex CLI accepts `--limit` values of 1–20 only (20 is also its
/// default), so every page request stays inside that window and larger
/// requests paginate with `--cursor` instead.
const CODEX_PAGE_LIMIT: usize = 20;

/// The process side of `codex cloud list --json`, abstracted so parsing,
/// mapping, and pagination are testable without the Codex CLI installed.
/// `limit` is a per-page cap (at most [`CODEX_PAGE_LIMIT`]); `cursor`
/// continues a previous page's listing.
pub trait CodexCloudLister: Send + Sync {
    fn list_json(&self, limit: usize, cursor: Option<&str>) -> Result<String>;
}

/// Runs the real `codex` CLI. Its `--json` output is Codex's documented
/// scripting contract, and the CLI handles auth/refresh itself — the same
/// reason the engine shells out to `git` instead of reimplementing it.
struct ExecCodexCli;

impl CodexCloudLister for ExecCodexCli {
    fn list_json(&self, limit: usize, cursor: Option<&str>) -> Result<String> {
        let mut command = std::process::Command::new("codex");
        command.args(["cloud", "list", "--json", "--limit"]);
        command.arg(limit.clamp(1, CODEX_PAGE_LIMIT).to_string());
        if let Some(cursor) = cursor {
            command.arg("--cursor").arg(cursor);
        }
        let output = command
            .stdin(std::process::Stdio::null())
            .output()
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::NotFound => anyhow::anyhow!(
                    "the `codex` CLI is not on PATH; install Codex to list cloud tasks"
                ),
                _ => anyhow::Error::from(error).context("could not run `codex cloud list`"),
            })?;
        anyhow::ensure!(
            output.status.success(),
            "`codex cloud list --json` failed ({}): {}",
            output.status,
            excerpt_one_line(&String::from_utf8_lossy(&output.stderr))
        );
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

/// Map one cloud task from `codex cloud list --json` into a catalog row.
fn map_codex_cloud_task(value: &Value) -> Option<(Candidate, ShallowSession)> {
    let id = string_field(value, "id")?;
    let updated_at = string_field(value, "updated_at");
    let last_activity_ms = updated_at.as_deref().and_then(crate::parse_iso_ms);
    // Status participates in the stamp: an applied or failed task whose
    // timestamp did not move still deserves a re-read.
    let stamp = format!(
        "cloud:{}:{}",
        updated_at.as_deref().unwrap_or("unknown"),
        string_field(value, "status")
            .as_deref()
            .unwrap_or("unknown")
    );
    let session = ShallowSession {
        source: "codex".into(),
        session_id: id.clone(),
        // The task title is Codex's own rendering of the prompt that created
        // the task — the only human-readable identifier the listing offers.
        // Bounded like every stored excerpt.
        first_prompt: string_field(value, "title").map(|title| excerpt(&title)),
        last_activity_ms,
        raw_path: string_field(value, "url"),
        discovery_state: "shallow".into(),
        ..Default::default()
    };
    let candidate = Candidate {
        source: "codex",
        locator: id.clone(),
        session_id: Some(id),
        recency_hint_ms: last_activity_ms,
        stamp,
    };
    Some((candidate, session))
}

/// One page of `codex cloud list --json` output.
struct CodexCloudPage {
    tasks: Vec<Value>,
    /// Continuation cursor, when the payload carries one.
    cursor: Option<String>,
}

/// Accept both documented shapes: `{"tasks": […], "cursor": …}` and a bare
/// task array (which carries no cursor and therefore ends the walk).
fn parse_codex_cloud_listing(raw: &str) -> Result<CodexCloudPage> {
    let payload: Value =
        serde_json::from_str(raw).context("`codex cloud list --json` output is not JSON")?;
    match &payload {
        Value::Array(tasks) => Ok(CodexCloudPage {
            tasks: tasks.clone(),
            cursor: None,
        }),
        Value::Object(_) => Ok(CodexCloudPage {
            tasks: payload
                .get("tasks")
                .and_then(Value::as_array)
                .cloned()
                .context("`codex cloud list --json` output has no tasks array")?,
            cursor: string_field(&payload, "cursor"),
        }),
        _ => anyhow::bail!("`codex cloud list --json` output has no tasks array"),
    }
}

/// Shallow adapter over the Codex cloud task list.
pub struct CodexCloudProvider {
    lister: Box<dyn CodexCloudLister>,
    limit: Option<usize>,
    fetched: FetchedRows,
}

impl CodexCloudProvider {
    pub fn new(lister: Box<dyn CodexCloudLister>, limit: Option<usize>) -> Self {
        Self {
            lister,
            limit,
            fetched: FetchedRows::default(),
        }
    }
}

impl ShallowSessionProvider for CodexCloudProvider {
    fn check_available(&self, home: &Path) -> Result<()> {
        anyhow::ensure!(
            codex_auth_path(home).is_file(),
            "CONNECTOR_NOT_CONFIGURED: Codex credentials unavailable"
        );
        Ok(())
    }

    fn acquire(
        &self,
        _home: &Path,
        observation: &ai_hist::observations::SessionObservation,
    ) -> Result<RemoteSessionEvidence> {
        acquire_codex_remote_session(&observation.key.session_id)
    }

    fn connector_id(&self) -> &str {
        CODEX_CLOUD_CONNECTOR
    }
    fn source(&self) -> &'static str {
        "codex"
    }

    fn location(&self) -> SessionLocation {
        SessionLocation::Remote
    }

    fn enumerate(
        &self,
        _env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let mut candidates = Vec::new();
        let mut rows = BTreeMap::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_LIST_PAGES {
            let page_limit = match self.limit {
                Some(limit) => limit
                    .saturating_sub(candidates.len())
                    .clamp(1, CODEX_PAGE_LIMIT),
                None => CODEX_PAGE_LIMIT,
            };
            let raw = self.lister.list_json(page_limit, cursor.as_deref())?;
            let page = parse_codex_cloud_listing(&raw)?;
            for task in &page.tasks {
                if let Some((candidate, session)) = map_codex_cloud_task(task) {
                    rows.insert(candidate.locator.clone(), session);
                    candidates.push(candidate);
                }
            }
            cursor = page.cursor;
            let done = cursor.is_none()
                || page.tasks.is_empty()
                || self.limit.is_some_and(|limit| candidates.len() >= limit);
            if done {
                break;
            }
        }
        *self.fetched.lock().expect("remote connector row cache") = rows;
        Ok(candidates)
    }

    fn read_shallow(
        &self,
        _scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        Ok(take_fetched(&self.fetched, &candidate.locator))
    }
}

// ---------------------------------------------------------------------------
// cloud — org-wide recall, preserving the upstream provider's natural key
// ---------------------------------------------------------------------------

fn excerpt(text: &str) -> String {
    text.trim()
        .chars()
        .take(crate::discover::EXCERPT_MAX_CHARS)
        .collect()
}
#[cfg(test)]
mod tests;

/// Select exactly one adapter. Construction does not read credentials or invoke a transport.
pub fn provider(
    home: &Path,
    connector: &str,
    instance: &str,
    limit: Option<usize>,
) -> Result<Box<dyn ShallowSessionProvider>> {
    anyhow::ensure!(
        limit.is_none_or(|n| n <= 10_000),
        "INVALID_ARGUMENT: source limit exceeds 10000"
    );
    anyhow::ensure!(
        !instance.trim().is_empty() && instance.len() <= 512,
        "INVALID_ARGUMENT: invalid connector instance"
    );
    let inner: Box<dyn ShallowSessionProvider> = match connector {
        CLAUDE_WEB_CONNECTOR => Box::new(ClaudeWebProvider::new(
            claude_credentials_path(home),
            claude_api_base_url(),
            Box::new(UreqClaudeTransport),
            limit,
        )),
        CODEX_CLOUD_CONNECTOR => Box::new(CodexCloudProvider::new(Box::new(ExecCodexCli), limit)),
        _ => anyhow::bail!("INVALID_ARGUMENT: unknown provider source connector"),
    };
    Ok(Box::new(InstanceProvider {
        inner,
        instance: instance.into(),
    }))
}
struct InstanceProvider {
    inner: Box<dyn ShallowSessionProvider>,
    instance: String,
}
impl ShallowSessionProvider for InstanceProvider {
    fn connector_id(&self) -> &str {
        self.inner.connector_id()
    }
    fn connector_instance(&self) -> &str {
        &self.instance
    }
    fn source(&self) -> &'static str {
        self.inner.source()
    }
    /// Instance wrapping does not change what the wrapped connector covers.
    fn evidence_kinds(&self) -> &'static [ai_hist::EvidenceKind] {
        self.inner.evidence_kinds()
    }
    fn location(&self) -> SessionLocation {
        self.inner.location()
    }
    fn check_available(&self, home: &Path) -> Result<()> {
        self.inner.check_available(home)
    }
    fn acquire(
        &self,
        home: &Path,
        observation: &ai_hist::observations::SessionObservation,
    ) -> Result<RemoteSessionEvidence> {
        self.inner.acquire(home, observation)
    }
    fn enumerate(&self, env: &DiscoveryEnv<'_>, limit: Option<usize>) -> Result<Vec<Candidate>> {
        self.inner.enumerate(env, limit)
    }
    fn read_shallow(
        &self,
        env: &ScanEnv<'_>,
        conn: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        self.inner.read_shallow(env, conn, candidate)
    }
}
