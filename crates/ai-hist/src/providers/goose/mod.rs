//! Passive Goose schema-17 acquisition. See plans/history-providers/goose.md.
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
const ID: &str = "goose";
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
    message_ids: MessageIdOrigin::Mixed,
    usage: None,
};
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: "goose_schema17_v1",
    acquire,
    sweep,
    resolve_roots: roots::resolve,
    watch_roots: storage::watch_roots,
});
#[derive(Debug, Serialize, Deserialize)]
struct Session {
    native_id: String,
    id: String,
    namespace: String,
    title: String,
    model: Option<String>,
    cwd: Option<String>,
    parent: Option<String>,
    started: i64,
    latest: i64,
    messages: Vec<Message>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Message {
    row_id: i64,
    native_id: Option<String>,
    key: String,
    role: String,
    content: serde_json::Value,
    timestamp: i64,
    metadata: Option<serde_json::Value>,
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
        .ok_or_else(|| anyhow::anyhow!("missing Goose snapshot"))?;
    let session: Session = serde_json::from_value(snapshot.payload.clone())?;
    let path = ctx
        .path
        .ok_or_else(|| anyhow::anyhow!("missing Goose session path"))?;
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
    // SQLite changes, including WAL writes, require a complete coherent snapshot.
    // Always re-read: this also repairs lost rows without trusting a source cursor.
    for path in storage::databases(ctx.roots)? {
        if !storage::in_scope(&path, ctx.paths) {
            continue;
        }
        crate::ingest::check_capture_cancelled()?;
        let ids = match storage::scan(&context, &path, |id, result| {
            let session = match result {
                Ok(session) => session,
                Err(error) => {
                    crate::ingest::check_capture_cancelled()?;
                    eprintln!("Goose {id}: {error:#}");
                    unread += 1;
                    return Ok(());
                }
            };
            // Each destination write either commits in full or rolls back,
            // without discarding healthy siblings from this source snapshot.
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
                    eprintln!("Goose {id}: {error:#}");
                    unread += 1;
                }
            }
            Ok(())
        }) {
            Ok(ids) => ids,
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("Goose {}: {error:#}", path.display());
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
            eprintln!("Goose {} retirement: {error:#}", path.display());
            unread += 1;
        }
    }
    if unread == 0 {
        unread += retire_missing_databases(&ctx)?;
    }
    Ok(SweepResult { inserted, unread })
}

fn retire_missing_databases(ctx: &SweepContext<'_>) -> Result<u64> {
    // A missing configured root is unavailable, not proof of deletion.
    let mut stmt = ctx.conn.prepare(
        "SELECT DISTINCT locator FROM (SELECT raw_path AS locator FROM sessions WHERE source=?1 AND raw_path IS NOT NULL UNION SELECT raw_locator AS locator FROM session_observations WHERE source=?1 AND location='local' AND raw_locator IS NOT NULL UNION SELECT raw_locator AS locator FROM session_presences WHERE source=?1 AND location='local' AND raw_locator IS NOT NULL)",
    )?;
    let paths = stmt
        .query_map([ID], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut unread = 0;
    for path in paths {
        let path = PathBuf::from(path);
        if !storage::in_scope(&path, ctx.paths) {
            continue;
        }
        let retire = || -> Result<()> {
            if storage::confirmed_missing(ctx.roots, &path)? {
                crate::ingest::check_capture_cancelled()?;
                let tx = ctx.conn.unchecked_transaction()?;
                normalize::retire_deleted(&tx, &path, &[])?;
                tx.commit()?;
            }
            Ok(())
        };
        if let Err(error) = retire() {
            crate::ingest::check_capture_cancelled()?;
            eprintln!("Goose {} retirement: {error:#}", path.display());
            unread += 1;
        }
    }
    Ok(unread)
}
