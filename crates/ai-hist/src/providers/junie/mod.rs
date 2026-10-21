//! Passive acquisition of Junie CLI 3110.7 session journals.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;

mod format;
mod normalize;
mod roots;
mod storage;
#[cfg(test)]
mod tests;

const ID: &str = "junie";
pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| Box::new(Provider::default())),
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
    generation: "junie_3110_7_v1",
    acquire,
    sweep,
    resolve_roots: roots::resolve,
    watch_roots: |roots| {
        roots
            .for_provider(ID)
            .iter()
            .cloned()
            .map(WatchRoot::tree)
            .collect()
    },
});

#[derive(Default)]
struct Provider {
    indexes: storage::IndexCache,
}
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
        self.indexes.clear();
        storage::enumerate(env, limit)
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
        let capture = storage::shallow(scan, &context, &path, &self.indexes)?;
        let sid = format::string(&capture.payload, "sid")?;
        let previous: Option<ShallowSession> = if let Some(conn) = conn {
            normalize::check_identity(conn, sid, &path)?;
            if capture.payload["partial"] == true || capture.payload["bounded"] == true {
                crate::discover::catalog_row(conn, ID, sid, true)?
            } else {
                None
            }
        } else {
            None
        };
        let rows = capture.payload["rows"].as_array().context("Junie rows")?;
        let summary = &capture.payload["summary"];
        let first = rows.iter().map(format::time).collect::<Result<Vec<_>>>()?;
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid.into(),
            first_activity_ms: summary["createdAt"]
                .as_i64()
                .or_else(|| first.iter().min().copied())
                .or_else(|| previous.as_ref().and_then(|row| row.first_activity_ms)),
            last_activity_ms: summary["updatedAt"]
                .as_i64()
                .or_else(|| first.iter().max().copied())
                .or_else(|| previous.as_ref().and_then(|row| row.last_activity_ms)),
            first_prompt: rows
                .iter()
                .find_map(format::human_text)
                .map(|s| s.chars().take(4096).collect())
                .or_else(|| previous.as_ref().and_then(|row| row.first_prompt.clone())),
            cwd: summary["projectDir"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| previous.as_ref().and_then(|row| row.cwd.clone())),
            title: summary["taskName"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| previous.as_ref().and_then(|row| row.title.clone())),
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
    let capture = storage::capture(path, context)?;
    anyhow::ensure!(
        capture.payload["sid"] == options.session_id,
        "JUNIE_ID_MISMATCH: selected session changed"
    );
    Ok(capture)
}

fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let capture = ctx.provider_session.context("missing Junie acquisition")?;
    normalize::write(ctx.conn, &capture.payload)?;
    let diagnostics = if capture.payload["partial"] == true {
        vec![crate::hydrate::HydrationDiagnostic {
            code: "JUNIE_INCOMPLETE_JOURNAL".into(),
            message: "Incomplete final event; replacement deferred until the journal is complete"
                .into(),
            duration_ms: None,
            source_bytes: Some(capture.bytes),
            records_parsed: Some(capture.records),
        }]
    } else {
        vec![]
    };
    Ok((
        IngestOutcome {
            records: capture.records,
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
    let indexes = storage::IndexCache::default();
    for candidate in storage::enumerate(&env, None)? {
        crate::ingest::check_capture_cancelled()?;
        let run = || -> Result<(usize, bool)> {
            let capture =
                storage::capture_with_index(Path::new(&candidate.locator), &context, &indexes)?;
            let tx = ctx.conn.unchecked_transaction()?;
            let n = normalize::write(&tx, &capture.payload)?;
            tx.commit()?;
            Ok((n, capture.payload["partial"] == true))
        };
        match run() {
            Ok((n, partial)) => {
                result.inserted += n;
                result.unread += u64::from(partial);
            }
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("Junie {}: {error:#}", candidate.locator);
                result.unread += 1;
                failed_capture = true;
            }
        }
    }
    if ctx.paths.is_none() && !failed_capture {
        crate::providers::file_history::retire_missing(ctx.conn, ID, ctx.roots)?;
    }
    Ok(result)
}
