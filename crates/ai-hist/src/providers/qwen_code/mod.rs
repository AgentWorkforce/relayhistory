//! Passive Qwen Code ordinary JSONL history; no agent or model calls.
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
mod storage;
#[cfg(test)]
mod tests;
mod usage;
const ID: &str = "qwen-code";
const HEAD_LIMIT: u64 = 64 * 1024;
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
    message_ids: MessageIdOrigin::Provider,
    usage: Some(UsageRegistration {
        accounting: UsageAccounting::PerMessage,
        normalize: usage::normalize,
        splits_requests: false,
    }),
};
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: "qwen_code_transcript_v1",
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
            EvidenceKind::ToolCall,
            EvidenceKind::SessionMarker,
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
        context.validate(Path::new(&candidate.locator))?;
        let path = Path::new(&candidate.locator).to_owned();
        let storage::ShallowRead {
            rows,
            latest,
            partial,
            bounded,
        } = storage::shallow_rows(scan, &path)?;
        let native = path
            .file_stem()
            .and_then(|v| v.to_str())
            .context("QWEN_CODE_INVALID_PATH: session filename")?;
        anyhow::ensure!(
            rows.iter().all(|r| r["sessionId"] == native),
            "QWEN_CODE_ID_MISMATCH: filename and record session disagree"
        );
        let sid = roots::identity(scan.provider_roots, &path)?;
        if let Some(conn) = conn {
            normalize::check_identity(conn, &sid, &path)?;
            if (partial || bounded) && normalize::has_evidence(conn, &sid)? {
                if let Some(cached) = crate::discover::catalog_row(conn, ID, &sid, true)? {
                    return Ok(Some(cached));
                }
            }
        }
        // A bounded prefix cannot prove the active branch: a later rewind
        // may select a different first prompt. Full projection errors stay visible.
        let projected = if !bounded && !partial {
            let active = format::project(&rows)?;
            anyhow::ensure!(
                !active.is_empty(),
                "QWEN_CODE_NO_CONVERSATION: no conversation records"
            );
            Some(active)
        } else {
            None
        };
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid,
            first_activity_ms: rows
                .iter()
                .map(format::time)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .min(),
            last_activity_ms: latest,
            first_prompt: projected
                .as_ref()
                .and_then(|active| active.iter().find(|r| format::role(r) == Some("user")))
                .map(format::text)
                .filter(|s| !s.is_empty()),
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
        "QWEN_CODE_ID_MISMATCH: selected conversation changed"
    );
    Ok(snapshot)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let captured = ctx
        .provider_session
        .context("missing Qwen Code acquisition")?;
    normalize::write(ctx.conn, &captured.payload)?;
    let diagnostics = if captured.payload["partial"] == true {
        vec![crate::hydrate::HydrationDiagnostic {
            code: "QWEN_CODE_INCOMPLETE_TRANSCRIPT".into(),
            message: "Incomplete final JSONL record deferred; prior complete evidence retained"
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
    // Replay the whole parent tree: an appended rewind can change which
    // earlier records belong to the active branch. Replacements also reconcile.
    for candidate in storage::enumerate(&env)? {
        crate::ingest::check_capture_cancelled()?;
        let run = || -> Result<(usize, bool)> {
            let captured = storage::capture(Path::new(&candidate.locator), &context)?;
            if captured.payload["partial"] == true
                && normalize::has_evidence(ctx.conn, format::string(&captured.payload, "sid")?)?
            {
                return Ok((0, true));
            }
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
                eprintln!("Qwen Code {}: {error:#}", candidate.locator);
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
