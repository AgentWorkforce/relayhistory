//! Connector dispatch: select exactly one adapter by id. Construction does
//! not read credentials or invoke a transport.
use std::path::Path;

use ai_hist::SessionLocation;
use anyhow::Result;
use rusqlite::Connection;

use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};

use super::claude_web::{claude_api_base_url, ClaudeWebProvider, UreqClaudeTransport};
use super::cursor_cloud::cursor_provider;
use super::devin_auth::devin_provider;
use super::{
    claude_credentials_path, CodexCloudProvider, ExecCodexCli, RemoteSessionEvidence,
    CLAUDE_WEB_CONNECTOR, CODEX_CLOUD_CONNECTOR, CURSOR_CLOUD_CONNECTOR, DEVIN_CLOUD_CONNECTOR,
};

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
            UreqClaudeTransport,
            limit,
        )),
        CODEX_CLOUD_CONNECTOR => Box::new(CodexCloudProvider::new(ExecCodexCli, limit)),
        DEVIN_CLOUD_CONNECTOR => Box::new(devin_provider(home, limit)),
        CURSOR_CLOUD_CONNECTOR => Box::new(cursor_provider(home, limit)),
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
