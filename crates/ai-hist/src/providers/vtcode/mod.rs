//! Passive acquisition of released VT Code canonical workspace journals.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;
use std::io::{Read, Seek, SeekFrom};

mod format;
mod normalize;
mod roots;
mod usage;

const ID: &str = "vtcode";
const SHALLOW_BYTES: u64 = 64 * 1024;
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
        accounting: UsageAccounting::PerRequest,
        normalize: usage::normalize,
        splits_requests: false,
    }),
};
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: "vtcode_0_18_v1",
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
        env.provider_roots.ensure_provider_roots(ID)?;
        let mut found = Vec::new();
        for root in env.provider_roots.for_provider(ID) {
            if env.path_scope().is_some_and(|scope| !scope.mentions(root)) {
                continue;
            }
            match std::fs::metadata(root) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                result => {
                    result.with_context(|| format!("VT Code root {}", root.display()))?;
                }
            }
            for entry in std::fs::read_dir(root)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let path = entry.path().join("events.jsonl");
                if env.path_scope().is_some_and(|scope| !scope.mentions(&path)) {
                    continue;
                }
                if !path.is_file() {
                    // A partial native session must be diagnosed per candidate;
                    // it cannot hide valid siblings or permit missing retirement.
                    found.push(Candidate {
                        source: ID,
                        locator: path.to_string_lossy().into_owned(),
                        session_id: None,
                        recency_hint_ms: None,
                        stamp: "missing-journal".into(),
                    });
                    continue;
                }
                AcquisitionContext::new(ID, &env.provider_roots).validate(&path)?;
                let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&path)?;
                found.push(Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: None,
                    recency_hint_ms,
                    stamp,
                });
            }
        }
        found.sort_by(|a, b| a.locator.cmp(&b.locator));
        found.dedup_by(|a, b| a.locator == b.locator);
        Ok(found)
    }
    fn read_access(&self) -> crate::discover::ShallowReadAccess {
        crate::discover::ShallowReadAccess::Catalog
    }
    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        conn: Option<&Connection>,
        c: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let path = Path::new(&c.locator);
        ensure_journal(path)?;
        let (sid, native) = roots::identity(scan.provider_roots, path)?;
        let context = AcquisitionContext::new(ID, scan.provider_roots);
        context.validate(path)?;
        let (rows, prefix_rows) = shallow_rows(scan, path)?;
        scan.note_records(rows.len() as u64);
        normalize::validate_native(&rows, &native)?;
        if let Some(conn) = conn {
            normalize::check_identity(conn, &sid, path)?;
        }
        let first = format::first_prompt(&rows[..prefix_rows], &native);
        let times = rows
            .iter()
            .map(format::event_time)
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid,
            cwd: path
                .parent()
                .and_then(Path::parent)
                .and_then(Path::parent)
                .and_then(Path::parent)
                .map(|p| p.to_string_lossy().into_owned()),
            first_prompt: first.as_ref().map(|x| x.0.clone()),
            first_activity_ms: times.iter().flatten().min().copied(),
            last_activity_ms: times.iter().flatten().max().copied(),
            raw_path: Some(c.locator.clone()),
            source_stamp: Some(c.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}

fn shallow_rows(scan: &ScanEnv<'_>, path: &Path) -> Result<(Vec<Value>, usize)> {
    let mut file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    let mut head = Vec::new();
    (&mut file).take(SHALLOW_BYTES).read_to_end(&mut head)?;
    scan.note_open();
    scan.note_bytes(head.len() as u64);
    let head_end = head.iter().rposition(|b| *b == b'\n').map_or(0, |p| p + 1);
    anyhow::ensure!(
        head_end > 0,
        "VTCODE_SHALLOW_BOUND: no complete event in first 64 KiB"
    );
    let (mut rows, _) = format::parse(&head[..head_end])?;
    let prefix_rows = rows.len();
    if size > head.len() as u64 {
        let start = size.saturating_sub(SHALLOW_BYTES).max(head.len() as u64);
        file.seek(SeekFrom::Start(start))?;
        let mut tail = Vec::new();
        (&mut file).take(SHALLOW_BYTES).read_to_end(&mut tail)?;
        scan.note_bytes(tail.len() as u64);
        let offset = if head.last() == Some(&b'\n') && start == head.len() as u64 {
            0
        } else {
            tail.iter()
                .position(|b| *b == b'\n')
                .map_or(tail.len(), |p| p + 1)
        };
        if offset < tail.len() && tail[offset..].contains(&b'\n') {
            rows.extend(format::parse(&tail[offset..])?.0);
        }
    }
    Ok((rows, prefix_rows))
}

fn ensure_journal(path: &Path) -> Result<()> {
    anyhow::ensure!(
        path.is_file(),
        "VTCODE_MISSING_JOURNAL: session directory {} has no events.jsonl",
        path.parent().unwrap_or(path).display()
    );
    Ok(())
}

fn snapshot(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    ensure_journal(path)?;
    let bytes = context.read(path)?;
    let (rows, partial) = format::parse(&bytes)?;
    let (sid, native) = roots::identity(context.roots, path)?;
    normalize::validate_native(&rows, &native)?;
    let records = rows.len() as i64;
    Ok(AcquiredSession::new(
        serde_json::json!({"sid":sid,"native":native,
        "rows":rows,"partial":partial}),
        bytes.len() as i64,
        records,
        vec![path.into()],
    ))
}

fn acquire(
    options: &HydrateSessionOptions,
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<AcquiredSession> {
    let captured = snapshot(path, context)?;
    anyhow::ensure!(
        captured.payload["sid"] == options.session_id,
        "VTCODE_ID_MISMATCH: selected native session changed"
    );
    Ok(captured)
}

fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let captured = ctx
        .provider_session
        .context("VT Code acquisition snapshot missing")?;
    let path = ctx.path.context("VT Code selected path missing")?;
    let inserted = normalize::write(ctx.conn, &captured.payload, path)?;
    let partial = captured.payload["partial"] == true;
    let diagnostics = partial
        .then(|| crate::hydrate::HydrationDiagnostic {
            code: "VTCODE_PARTIAL_TAIL".into(),
            message: "Incomplete final native event deferred; complete prefix retained".into(),
            duration_ms: None,
            source_bytes: Some(captured.bytes),
            records_parsed: Some(captured.records),
        })
        .into_iter()
        .collect();
    Ok((
        IngestOutcome {
            records: inserted as i64,
            ..Default::default()
        },
        diagnostics,
        None,
    ))
}

fn sweep(ctx: SweepContext<'_>) -> Result<SweepResult> {
    ctx.roots.ensure_provider_roots(ID)?;
    let env = DiscoveryEnv::with_provider_roots(ctx.conn, ctx.roots.clone())
        .with_path_scope(ctx.paths.cloned());
    let mut result = SweepResult::default();
    for candidate in Provider.enumerate(&env, None)? {
        crate::ingest::check_capture_cancelled()?;
        let path = Path::new(&candidate.locator);
        let run = || -> Result<(usize, bool)> {
            let captured = snapshot(path, &AcquisitionContext::new(ID, ctx.roots))?;
            let tx = ctx.conn.unchecked_transaction()?;
            let inserted = normalize::write(&tx, &captured.payload, path)?;
            tx.commit()?;
            Ok((inserted, captured.payload["partial"] == true))
        };
        match run() {
            Ok((inserted, partial)) => {
                result.inserted += inserted;
                result.unread += u64::from(partial);
            }
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("VT Code {}: {error:#}", path.display());
                result.unread += 1;
            }
        }
    }
    if ctx.paths.is_none() && result.unread == 0 {
        crate::providers::file_history::retire_missing(ctx.conn, ID, ctx.roots)?;
    }
    Ok(result)
}
