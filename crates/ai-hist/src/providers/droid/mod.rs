//! Passive Droid v2 JSONL history. No agent process, configuration or model access.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;
use std::io::Read;
mod blocks;
mod format;
mod normalize;
mod storage;
#[cfg(test)]
mod tests;

const ID: &str = "droid";
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
    usage: None,
};
const GENERATION: &str = "droid_v2_3";
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: GENERATION,
    acquire,
    sweep,
    resolve_roots: |home, use_env| resolve_roots(home, use_env).into(),
    watch_roots: |r| {
        r.for_provider(ID)
            .iter()
            .cloned()
            .map(WatchRoot::tree)
            .collect()
    },
});
fn resolve_roots(home: &Path, _: bool) -> Vec<PathBuf> {
    vec![home.join(".factory/sessions")]
}

fn prefix(context: &AcquisitionContext<'_>, path: &Path) -> Result<Vec<u8>> {
    context.validate(path)?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(HEAD_LIMIT)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
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
    fn enumerate(&self, env: &DiscoveryEnv<'_>, _: Option<usize>) -> Result<Vec<Candidate>> {
        let mut out = Vec::new();
        for root in env.provider_roots.for_provider(ID) {
            if env.path_scope().is_some_and(|s| !s.mentions(root)) {
                continue;
            }
            match std::fs::metadata(root) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                result => {
                    result.with_context(|| format!("Droid root {}", root.display()))?;
                }
            }
            for path in
                crate::ingest::collect_matching_files_scoped(root, "", "jsonl", env.path_scope())?
            {
                if !storage::is_transcript(root, &path) {
                    continue;
                }
                // Keep a failing locator in the work list so shallow discovery
                // reports it individually and the sweep records unread evidence
                // without blocking healthy sessions or retiring prior snapshots.
                let (stamp, recency_hint_ms) =
                    storage::stamp(&path, &AcquisitionContext::new(ID, &env.provider_roots))
                        .unwrap_or_else(|_| ("droid_stamp_unavailable".into(), None));
                out.push(Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: None,
                    recency_hint_ms,
                    stamp,
                });
            }
        }
        out.sort_by(|a, b| a.locator.cmp(&b.locator));
        out.dedup_by(|a, b| a.locator == b.locator);
        Ok(out)
    }
    fn read_access(&self) -> crate::discover::ShallowReadAccess {
        // Native identity ownership must be checked against each preceding
        // catalog write, including two files in a fresh discovery run.
        crate::discover::ShallowReadAccess::Catalog
    }
    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        conn: Option<&Connection>,
        c: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let (source_stamp, _) = storage::stamp(
            Path::new(&c.locator),
            &AcquisitionContext::new(ID, scan.provider_roots),
        )?;
        let bytes = prefix(
            &AcquisitionContext::new(ID, scan.provider_roots),
            Path::new(&c.locator),
        )?;
        scan.note_open();
        scan.note_bytes(bytes.len() as u64);
        let tail = storage::tail(
            Path::new(&c.locator),
            &AcquisitionContext::new(ID, scan.provider_roots),
        )?;
        scan.note_open();
        scan.note_bytes(tail.len() as u64);
        anyhow::ensure!(
            storage::stamp(
                Path::new(&c.locator),
                &AcquisitionContext::new(ID, scan.provider_roots)
            )?
            .0 == source_stamp,
            "DROID_CAPTURE_CHANGED: retry after writer settles"
        );
        let entries = format::parse(&bytes, true)?;
        scan.note_records(entries.len() as u64);
        let (last_activity, tail_records) = format::last_activity(&tail)?;
        scan.note_records(tail_records as u64);
        let header = &entries[0];
        let sid = format::string(header, "id")?;
        if let Some(conn) = conn {
            normalize::check_identity(conn, sid, Path::new(&c.locator))?;
        }
        let first_prompt = format::first_prompt(&entries);
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid.into(),
            cwd: header["cwd"].as_str().map(str::to_owned),
            first_prompt,
            first_activity_ms: entries
                .iter()
                .filter_map(|e| format::time(e).ok().flatten())
                .min(),
            last_activity_ms: last_activity.or(c.recency_hint_ms).max(
                entries
                    .iter()
                    .filter_map(|e| format::time(e).ok().flatten())
                    .max(),
            ),
            title: header["sessionTitle"]
                .as_str()
                .or_else(|| header["title"].as_str())
                .map(str::to_owned),
            raw_path: Some(c.locator.clone()),
            source_stamp: Some(source_stamp),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}
fn snapshot(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    storage::snapshot(path, context)
}

fn acquire(
    options: &HydrateSessionOptions,
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<AcquiredSession> {
    let result = snapshot(path, context)?;
    anyhow::ensure!(
        result.payload["entries"][0]["id"].as_str() == Some(&options.session_id),
        "SESSION_SOURCE_MISMATCH: Droid native identity changed"
    );
    Ok(result)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let snapshot = ctx
        .provider_session
        .context("Droid acquisition snapshot missing")?;
    normalize::write(
        ctx.conn,
        &snapshot.payload,
        ctx.path.context("Droid path missing")?,
    )?;
    let diagnostics = if snapshot.payload["tail"]
        .as_array()
        .is_some_and(|a| !a.is_empty())
    {
        vec![crate::hydrate::HydrationDiagnostic {
            code: "DROID_PARTIAL_TAIL".into(),
            message: "Deferred incomplete final JSONL record; prior evidence retained".into(),
            duration_ms: None,
            source_bytes: Some(snapshot.bytes),
            records_parsed: Some(snapshot.records),
        }]
    } else {
        Vec::new()
    };
    Ok((
        IngestOutcome {
            records: snapshot.records,
            ..Default::default()
        },
        diagnostics,
        None,
    ))
}
fn sweep(ctx: SweepContext<'_>) -> Result<SweepResult> {
    let env = DiscoveryEnv::with_provider_roots(ctx.conn, ctx.roots.clone())
        .with_path_scope(ctx.paths.cloned());
    let mut result = SweepResult::default();
    let cache_key = "droid_file_stamps";
    let mut cache = ctx
        .state
        .get(cache_key)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let candidates = Provider.enumerate(&env, None)?;
    if ctx.paths.is_none() {
        let visited: std::collections::BTreeSet<_> =
            candidates.iter().map(|c| c.locator.as_str()).collect();
        cache.retain(|path, _| visited.contains(path.as_str()));
    }
    for candidate in candidates {
        crate::ingest::check_capture_cancelled()?;
        let path = Path::new(&candidate.locator);
        let generation = GENERATION;
        let cached_sid = cache
            .get(&candidate.locator)
            .filter(|previous| {
                previous["stamp"].as_str() == Some(&candidate.stamp)
                    && previous["generation"].as_str() == Some(generation)
            })
            .and_then(|previous| previous["sid"].as_str());
        if let Some(sid) = cached_sid {
            if !ctx.repairs.contains(ID, sid) {
                let present: bool = ctx.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sessions WHERE source='droid' AND session_id=?1)",
                    [sid],
                    |r| r.get(0),
                )?;
                if present {
                    continue;
                }
            }
        }
        let ingest = || -> Result<(usize, bool, String)> {
            let snapshot = snapshot(path, &AcquisitionContext::new(ID, ctx.roots))?;
            let tx = ctx.conn.unchecked_transaction()?;
            let inserted = normalize::write(&tx, &snapshot.payload, path)?;
            tx.commit()?;
            Ok((
                inserted,
                snapshot.payload["tail"]
                    .as_array()
                    .is_some_and(|a| !a.is_empty()),
                format::string(&snapshot.payload["entries"][0], "id")?.to_owned(),
            ))
        };
        match ingest() {
            Ok((n, partial, sid)) => {
                if partial {
                    cache.remove(&candidate.locator);
                } else {
                    cache.insert(candidate.locator.clone(), serde_json::json!({"stamp": candidate.stamp, "generation": generation, "sid": sid}));
                }
                result.inserted += n;
                result.unread += u64::from(partial);
            }
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                crate::ingest::provider_diagnostics::provider_diagnostic(
                    crate::ingest::provider_diagnostics::ProviderSkip {
                        source: crate::Source::Droid,
                        code: "EVIDENCE_PARTIAL",
                        subject: Some(&candidate.locator),
                        what: format_args!("skipping Droid transcript {}", path.display()),
                        detail: format_args!("{error:#}"),
                    },
                );
                cache.remove(&candidate.locator);
                result.unread += 1;
            }
        }
    }
    if ctx.paths.is_none() && result.unread == 0 {
        crate::providers::file_history::retire_missing(ctx.conn, ID, ctx.roots)?;
    }
    ctx.state.insert(cache_key.into(), Value::Object(cache));
    Ok(result)
}
