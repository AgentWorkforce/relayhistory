//! Passive Hermes schema-31 acquisition. See plans/history-providers/hermes.md.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use serde::{Deserialize, Serialize};
mod normalize;
mod roots;
mod storage;
#[cfg(test)]
mod tests;
const ID: &str = "hermes";
pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| Box::new(Provider)),
    hydration: Hydration::Parser(ingest),
    transcript_roots: |roots| roots.for_provider(ID).to_vec(),
    relationships: Relationships::NONE,
    resume: None,
    fixtures: Fixtures::Dir(ID),
};
pub(crate) const METADATA: ProviderMetadata = ProviderMetadata {
    markers: true,
    message_ids: MessageIdOrigin::Provider,
    usage: None,
};
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: "hermes_schema31_v1",
    acquire,
    sweep,
    resolve_roots: roots::resolve,
    watch_roots: storage::watch_roots,
});
#[derive(Debug, Serialize, Deserialize)]
struct Session {
    native_id: String,
    id: String,
    profile: String,
    title: Option<String>,
    model: Option<String>,
    cwd: Option<String>,
    git_branch: Option<String>,
    parent: Option<String>,
    end_reason: Option<String>,
    started: i64,
    ended: Option<i64>,
    messages: Vec<Message>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Message {
    id: i64,
    role: String,
    content: Option<String>,
    timestamp: i64,
    tool_call_id: Option<String>,
    tool_calls: Option<String>,
    tool_name: Option<String>,
    reasoning: Option<String>,
    reasoning_content: Option<String>,
    finish_reason: Option<String>,
    compressed: bool,
    active: bool,
    compacted: bool,
    observed: bool,
    display_kind: Option<String>,
}
struct Provider;
impl ShallowSessionProvider for Provider {
    fn source(&self) -> &'static str {
        ID
    }
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        &[
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::SessionMarker,
        ]
    }
    fn enumerate(&self, env: &DiscoveryEnv<'_>, limit: Option<usize>) -> Result<Vec<Candidate>> {
        storage::enumerate(&env.provider_roots, limit)
    }
    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        _: Option<&Connection>,
        c: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        storage::shallow(scan, c)
    }
}
fn acquire(
    options: &HydrateSessionOptions,
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<AcquiredSession> {
    storage::capture(context, path, &options.session_id)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let snapshot = ctx
        .provider_session
        .ok_or_else(|| anyhow::anyhow!("missing Hermes snapshot"))?;
    let session: Session = serde_json::from_value(snapshot.payload.clone())?;
    normalize::session(ctx.conn, &session, ctx.path.unwrap())?;
    Ok((
        IngestOutcome {
            records: snapshot.records,
            ..Default::default()
        },
        vec![],
        None,
    ))
}
fn sweep(ctx: SweepContext<'_>) -> Result<SweepResult> {
    let context = AcquisitionContext::new(ID, ctx.roots);
    let mut inserted = 0;
    // SQLite changes, including WAL writes, require a complete coherent snapshot.
    // Always re-read: this also repairs lost rows without trusting a source cursor.
    for path in storage::databases(ctx.roots)? {
        let sessions = storage::all(&context, &path)?;
        for session in sessions {
            let tx = ctx.conn.unchecked_transaction()?;
            inserted += normalize::session(&tx, &session, &path)?;
            tx.commit()?;
        }
    }
    Ok(SweepResult {
        inserted,
        unread: 0,
    })
}
