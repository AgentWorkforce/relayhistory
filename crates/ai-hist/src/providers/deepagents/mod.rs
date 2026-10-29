//! Passive DeepAgents Code SQLite checkpoint acquisition.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use serde::{Deserialize, Serialize};
mod decode;
#[cfg(test)]
mod history_identity_tests;
mod lifecycle;
mod normalize;
mod roots;
mod storage;
#[cfg(test)]
mod tests;
const ID: &str = "deepagents";
pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| Box::new(Provider)),
    hydration: Hydration::Parser(ingest),
    transcript_roots: |roots| {
        roots
            .for_provider(ID)
            .iter()
            .filter_map(|p| p.parent().map(Path::to_path_buf))
            .collect()
    },
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
    generation: "dcode_checkpoint_v2",
    acquire,
    sweep,
    resolve_roots: roots::resolve,
    watch_roots: lifecycle::watch_roots,
});
#[derive(Debug, Serialize, Deserialize)]
struct Session {
    native_id: String,
    id: String,
    started: i64,
    latest: i64,
    title: Option<String>,
    cwd: Option<String>,
    git_branch: Option<String>,
    checkpoint_id: String,
    messages: Vec<Message>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Message {
    id: String,
    role: String,
    content: String,
    tool_call_id: Option<String>,
    tool_calls: Vec<serde_json::Value>,
    status: Option<String>,
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
    fn fingerprint_inputs(&self, env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
        storage::fingerprint_inputs(&env.provider_roots)
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
        .ok_or_else(|| anyhow::anyhow!("missing DeepAgents snapshot"))?;
    let session: Session = serde_json::from_value(snapshot.payload.clone())?;
    let path = ctx
        .path
        .ok_or_else(|| anyhow::anyhow!("missing DeepAgents database path"))?;
    normalize::session(ctx.conn, &session, path)?;
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
    let (paths, mut unread) = lifecycle::sweep_databases(ctx.roots, ctx.paths)?;
    for path in paths {
        crate::ingest::check_capture_cancelled()?;
        // Keep one SQLite source snapshot while committing each normalized
        // conversation independently. A malformed sibling must not undo a
        // healthy one, and decoded messages do not accumulate across threads.
        let capture = storage::visit(&context, &path, |session| {
            crate::ingest::check_capture_cancelled()?;
            match normalize::session(ctx.conn, &session, &path) {
                Ok(count) => {
                    inserted += count;
                    Ok(true)
                }
                Err(_) => {
                    crate::ingest::check_capture_cancelled()?;
                    Ok(false)
                }
            }
        });
        let visit = match capture {
            Ok(visit) => visit,
            Err(_) => {
                crate::ingest::check_capture_cancelled()?;
                unread += 1;
                continue;
            }
        };
        unread += visit.unread;
        if visit.unread == 0 {
            let ids = visit
                .ids
                .iter()
                .map(|native| storage::identity(ctx.roots, &path, native))
                .collect::<Vec<_>>();
            let retirement = (|| -> Result<()> {
                let tx = ctx.conn.unchecked_transaction()?;
                normalize::retire_deleted(&tx, &path, &ids)?;
                tx.commit()?;
                Ok(())
            })();
            if retirement.is_err() {
                crate::ingest::check_capture_cancelled()?;
                unread += 1;
            }
        }
    }
    unread += lifecycle::retire_missing_databases(&ctx)?;
    Ok(SweepResult { inserted, unread })
}
