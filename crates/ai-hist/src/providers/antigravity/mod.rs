//! Passive Antigravity native transcript acquisition; no agent or model calls.
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
mod summary;
#[cfg(test)]
mod tests;
const ID: &str = "antigravity";
const HEAD_LIMIT: u64 = 64 * 1024;
pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| Box::new(Provider::default())),
    hydration: Hydration::Parser(ingest),
    transcript_roots: roots::transcript_roots,
    // A parent conversation writes `.system_generated/subagents/<child>.json`
    // per spawned subagent; its `conversationId` names the child's own
    // conversation under the same brain root.
    relationships: Relationships {
        stable_child_identity: "always",
        records_agent_type: true,
        records_spawn_time: true,
        records_evidence_locator: true,
    },
    resume: None,
    fixtures: Fixtures::Dir(ID),
};
pub(crate) const METADATA: ProviderMetadata = ProviderMetadata {
    markers: true,
    message_ids: MessageIdOrigin::Synthesized,
    usage: None,
};
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: "antigravity_transcript_v4",
    acquire,
    sweep,
    resolve_roots: roots::resolve,
    watch_roots: |r| {
        let mut out = Vec::new();
        for brain in r.for_provider(ID) {
            out.push(WatchRoot::tree(brain.clone()));
            if let Some(dir) = roots::summary_dir_for_brain(brain) {
                out.push(WatchRoot::directory_of(
                    dir,
                    crate::discover::WatchEntries::OpencodeStores {
                        primary: roots::SUMMARY_DB.into(),
                        channels: false,
                    },
                ));
            }
        }
        out
    },
});
#[derive(Default)]
struct Provider {
    titles: std::sync::Mutex<
        std::collections::HashMap<PathBuf, std::collections::HashMap<String, String>>,
    >,
}
impl Provider {
    fn title_for(&self, path: &Path, context: &AcquisitionContext<'_>) -> Option<String> {
        let (brain_root, native_id) = roots::locate(context.roots, path).ok()?;
        let mut cache = self
            .titles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let titles = cache
            .entry(brain_root.to_path_buf())
            .or_insert_with(|| summary::read_titles_for_brain(brain_root, context));
        titles.get(&native_id).cloned()
    }
}
impl ShallowSessionProvider for Provider {
    fn source(&self) -> &'static str {
        ID
    }
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        &[
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::SessionMarker,
            EvidenceKind::Relationship,
        ]
    }
    // A subagent sidecar or summary DB update can arrive while every
    // transcript stays byte-identical; fold them in so such a tick is not skipped.
    fn fingerprint_inputs(&self, env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
        let mut inputs = storage::enumerate_for_fingerprint(env)?;
        inputs.extend(storage::subagent_candidates(&inputs)?);
        inputs.extend(summary::fingerprint_candidates(env)?);
        Ok(inputs)
    }
    fn enumerate(&self, env: &DiscoveryEnv<'_>, _: Option<usize>) -> Result<Vec<Candidate>> {
        let (candidates, titles) = storage::enumerate_with_titles(env)?;
        *self
            .titles
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = titles;
        Ok(candidates)
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
        let (rows, latest) = storage::shallow_rows(scan, &path)?;
        let sid = roots::identity(scan.provider_roots, &path)?;
        let title = self.title_for(&path, &context);
        if let Some(conn) = conn {
            normalize::check_identity(conn, &sid, &path)?;
            // `conversation_summaries.db` is Antigravity's current title
            // authority. The generic shallow upsert preserves existing full
            // titles for providers whose shallow reads may see stale prefixes;
            // this value comes straight from the summary database.
            crate::ingest::titles::set_session_title(conn, ID, &sid, title.as_deref())?;
        }
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid,
            title,
            first_activity_ms: rows
                .iter()
                .map(format::time)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .min(),
            last_activity_ms: latest,
            first_prompt: rows
                .iter()
                .filter(|r| {
                    format::role(r) == Some("user")
                        && r["status"] == "DONE"
                        && r["is_truncated"] != true
                        && r["truncated_fields"]
                            .as_array()
                            .is_none_or(|fields| fields.is_empty())
                })
                .find_map(|r| format::text(r).filter(|s| !s.is_empty()))
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
        "ANTIGRAVITY_ID_MISMATCH: selected conversation changed"
    );
    Ok(snapshot)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let captured = ctx
        .provider_session
        .context("missing Antigravity acquisition")?;
    normalize::write(ctx.conn, &captured.payload)?;
    let diagnostics = if captured.payload["partial"] == true {
        vec![crate::hydrate::HydrationDiagnostic { code:"ANTIGRAVITY_INCOMPLETE_TRANSCRIPT".into(),message:"Incomplete final record or compact truncation deferred; prior complete evidence retained".into(),duration_ms:None,source_bytes:Some(captured.bytes),records_parsed:Some(captured.records) }]
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
    for candidate in storage::enumerate_for_sweep(&env)? {
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
                eprintln!("Antigravity {}: {error:#}", candidate.locator);
                result.unread += 1;
                failed_capture = true;
            }
        }
    }
    // Scoped summary DB updates reconcile titles directly against cataloged
    // sessions rather than replaying every transcript in the brain root.
    if let Some(scope) = ctx.paths {
        let tx = ctx.conn.unchecked_transaction()?;
        for root in ctx.roots.for_provider(ID) {
            if summary::scope_mentions(scope, root) {
                summary::reconcile_titles(&tx, root, &context)?;
            }
        }
        tx.commit()?;
    }
    // Successful partial captures keep the source fingerprint incomplete, but
    // their live locators do not make unrelated missing conversations uncertain.
    // Actual capture failures still prevent whole-source retirement.
    if ctx.paths.is_none() && !failed_capture {
        crate::providers::file_history::retire_missing(ctx.conn, ID, ctx.roots)?;
    }
    Ok(result)
}
