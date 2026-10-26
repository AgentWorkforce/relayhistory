//! Passive Kilo Code CLI 7.8.15 history acquisition.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use serde::{Deserialize, Serialize};
mod normalize;
mod projections;
mod prompts;
mod roots;
mod shallow;
mod storage;
#[cfg(test)]
mod tests;
const ID: &str = "kilo";
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
    generation: "kilo_sqlite_7_8_15_v1",
    acquire,
    sweep,
    resolve_roots: roots::resolve,
    watch_roots: roots::watch_roots,
});
#[derive(Debug, Serialize, Deserialize)]
struct Session {
    native_id: String,
    id: String,
    namespace: String,
    title: String,
    cwd: String,
    parent: Option<String>,
    started: i64,
    latest: i64,
    invalid_metadata: bool,
    messages: Vec<Message>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Message {
    id: String,
    role: String,
    timestamp: i64,
    data: serde_json::Value,
    parts: Vec<Part>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Part {
    id: String,
    timestamp: i64,
    data: serde_json::Value,
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
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        storage::shallow(scan, candidate)
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
        .ok_or_else(|| anyhow::anyhow!("missing Kilo snapshot"))?;
    let session: Session = serde_json::from_value(snapshot.payload.clone())?;
    let path = ctx
        .path
        .ok_or_else(|| anyhow::anyhow!("missing Kilo locator"))?;
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
    let mut unread = 0;
    let (databases, broken_paths) = storage::databases(ctx.roots)?;
    unread += broken_paths;
    for path in databases {
        if !storage::in_scope(&path, ctx.paths) {
            continue;
        }
        crate::ingest::check_capture_cancelled()?;
        let ids = match storage::scan(&context, &path, |id, result| {
            let session = match result {
                Ok(value) => value,
                Err(error) => {
                    crate::ingest::check_capture_cancelled()?;
                    eprintln!("Kilo {id}: {error:#}");
                    unread += 1;
                    return Ok(());
                }
            };
            let write = || -> Result<usize> {
                let tx = ctx.conn.unchecked_transaction()?;
                let count = normalize::session(&tx, &session, &path)?;
                tx.commit()?;
                Ok(count)
            };
            match write() {
                Ok(count) => inserted += count,
                Err(error) => {
                    crate::ingest::check_capture_cancelled()?;
                    eprintln!("Kilo {id}: {error:#}");
                    unread += 1;
                }
            }
            Ok(())
        }) {
            Ok(ids) => ids,
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("Kilo {}: {error:#}", path.display());
                unread += 1;
                continue;
            }
        };
        let retire = || -> Result<()> {
            let tx = ctx.conn.unchecked_transaction()?;
            normalize::retire_deleted(&tx, &path, &ids)?;
            tx.commit()?;
            Ok(())
        };
        if let Err(error) = retire() {
            crate::ingest::check_capture_cancelled()?;
            eprintln!("Kilo {} retirement: {error:#}", path.display());
            unread += 1;
        }
    }
    if unread == 0 {
        unread += retire_missing_databases(&ctx)?;
    }
    Ok(SweepResult { inserted, unread })
}
fn retire_missing_databases(ctx: &SweepContext<'_>) -> Result<u64> {
    let mut stmt = ctx.conn.prepare("SELECT DISTINCT locator FROM (SELECT raw_path AS locator FROM sessions WHERE source=?1 AND raw_path IS NOT NULL UNION SELECT raw_locator AS locator FROM session_observations WHERE source=?1 AND location='local' AND raw_locator IS NOT NULL UNION SELECT raw_locator AS locator FROM session_presences WHERE source=?1 AND location='local' AND raw_locator IS NOT NULL)")?;
    let paths = stmt
        .query_map([ID], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut unread = 0;
    for path in paths {
        crate::ingest::check_capture_cancelled()?;
        let path = PathBuf::from(path);
        if !storage::in_scope(&path, ctx.paths) {
            continue;
        }
        let retire = || -> Result<()> {
            if storage::confirmed_missing(ctx.roots, &path)? {
                let tx = ctx.conn.unchecked_transaction()?;
                normalize::retire_deleted(&tx, &path, &[])?;
                tx.commit()?;
            }
            Ok(())
        };
        if let Err(error) = retire() {
            crate::ingest::check_capture_cancelled()?;
            eprintln!("Kilo {} retirement: {error:#}", path.display());
            unread += 1;
        }
    }
    Ok(unread)
}
