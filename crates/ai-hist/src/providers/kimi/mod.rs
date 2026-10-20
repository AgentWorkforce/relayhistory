//! Passive Kimi native transcript acquisition; no agent or model calls.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;
use sha2::{Digest, Sha256};
mod format;
mod normalize;
mod roots;
mod shared_evidence;
mod storage;
#[cfg(test)]
mod tests;
const ID: &str = "kimi";
pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| Box::new(Provider)),
    hydration: Hydration::Parser(ingest),
    transcript_roots: |r| r.for_provider(ID).to_vec(),
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
    generation: "kimi_wire_1_5_v1",
    acquire,
    sweep,
    resolve_roots: roots::resolve,
    watch_roots: |r| {
        r.for_provider(ID)
            .iter()
            .cloned()
            .map(WatchRoot::tree)
            .collect()
    },
});
struct Provider;
impl ShallowSessionProvider for Provider {
    fn source(&self) -> &'static str {
        ID
    }
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        &[
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::SessionMarker,
            EvidenceKind::ToolCall,
        ]
    }
    fn enumerate(&self, env: &DiscoveryEnv<'_>, _: Option<usize>) -> Result<Vec<Candidate>> {
        storage::enumerate(env)
    }
    fn read_access(&self) -> crate::discover::ShallowReadAccess {
        crate::discover::ShallowReadAccess::Catalog
    }
    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        conn: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let context = AcquisitionContext::new(ID, scan.provider_roots);
        let path = storage::selected(&context, Path::new(&candidate.locator))?;
        let captured = storage::shallow(scan, &context, &path)?;
        let rows = captured.payload["rows"]
            .as_array()
            .context("Kimi records")?;
        let latest = rows
            .iter()
            .filter_map(|r| format::time(r).ok())
            .max()
            .context("Kimi timestamps")?;
        let sid = captured.payload["sid"]
            .as_str()
            .context("Kimi identity")?
            .to_owned();
        if let Some(conn) = conn {
            normalize::check_identity(conn, &sid, &path)?;
            if captured.payload["partial"] == true || captured.payload["bounded"] == true {
                if let Some(previous) = crate::discover::catalog_row(conn, ID, &sid, true)? {
                    return Ok(Some(previous));
                }
            }
        }
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid,
            first_activity_ms: rows
                .iter()
                .map(format::time)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .min(),
            last_activity_ms: Some(latest),
            first_prompt: if captured.payload["bounded"] == true
                || captured.payload["partial"] == true
            {
                None
            } else {
                let live = format::active_messages(rows)?;
                rows.iter()
                    .enumerate()
                    .filter(|(index, _)| live.contains(index))
                    .find_map(|(_, row)| format::human_text(row))
                    .map(|text| text.chars().take(4096).collect())
            },
            cwd: captured.payload["state"]["cwd"].as_str().map(str::to_owned),
            title: captured.payload["state"]["title"]
                .as_str()
                .map(str::to_owned),
            raw_path: Some(path.to_string_lossy().into_owned()),
            source_stamp: Some(candidate.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}
fn acquire(
    options: &HydrateSessionOptions,
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<AcquiredSession> {
    let snapshot = storage::capture(path, context)?;
    anyhow::ensure!(
        snapshot.payload["sid"] == options.session_id,
        "KIMI_ID_MISMATCH: selected conversation changed"
    );
    Ok(snapshot)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let captured = ctx.provider_session.context("missing Kimi acquisition")?;
    normalize::write(ctx.conn, &captured.payload)?;
    let diagnostics = if captured.payload["partial"] == true {
        vec![crate::hydrate::HydrationDiagnostic {
            code: "KIMI_INCOMPLETE_TRANSCRIPT".into(),
            message: "Incomplete final wire record; all replacement writes deferred until complete"
                .into(),
            duration_ms: None,
            source_bytes: Some(captured.bytes),
            records_parsed: Some(captured.records),
        }]
    } else {
        vec![]
    };
    Ok((
        IngestOutcome {
            records: captured.records,
            ..Default::default()
        },
        diagnostics,
        None,
    ))
}
fn sweep(ctx: SweepContext<'_>) -> Result<SweepResult> {
    let env = DiscoveryEnv::with_provider_roots(ctx.conn, ctx.roots.clone())
        .with_path_scope(ctx.paths.cloned());
    let context = AcquisitionContext::new(ID, ctx.roots);
    let mut result = SweepResult::default();
    let mut failed_capture = false;
    // Whole files are rewritten by the native writer. Replay the selected file
    // rather than trusting an append cursor or an ownership-blind stamp cache.
    for candidate in storage::enumerate(&env)? {
        crate::ingest::check_capture_cancelled()?;
        let run = || -> Result<(usize, bool)> {
            let captured = storage::capture(Path::new(&candidate.locator), &context)?;
            let tx = ctx.conn.unchecked_transaction()?;
            let n = normalize::write(&tx, &captured.payload)?;
            tx.commit()?;
            Ok((n, captured.payload["partial"] == true))
        };
        match run() {
            Ok((n, partial)) => {
                result.inserted += n;
                result.unread += u64::from(partial);
            }
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("Kimi {}: {error:#}", candidate.locator);
                result.unread += 1;
                failed_capture = true;
            }
        }
    }
    // Successful partial captures keep the source fingerprint incomplete, but
    // their live locators do not make unrelated missing conversations uncertain.
    // Actual capture failures still prevent whole-source retirement.
    if ctx.paths.is_none() && !failed_capture {
        crate::providers::file_history::retire_missing(ctx.conn, ID, ctx.roots)?;
    }
    Ok(result)
}
