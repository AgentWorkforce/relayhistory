//! Passive acquisition of Mistral Vibe 3.0.1 Unified session stores.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;

mod normalize;
mod roots;
mod storage;

const ID: &str = "mistral-vibe";
const GENERATION: &str = "mistral_vibe_unified_v1_minor9_v1";

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
    generation: GENERATION,
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
    fn enumerate(&self, env: &DiscoveryEnv<'_>, limit: Option<usize>) -> Result<Vec<Candidate>> {
        env.provider_roots.ensure_provider_roots(ID)?;
        let mut found = Vec::new();
        for root in env.provider_roots.for_provider(ID) {
            match std::fs::metadata(root) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                result => {
                    result.with_context(|| format!("Vibe root {}", root.display()))?;
                }
            }
            for entry in std::fs::read_dir(root)? {
                crate::ingest::check_capture_cancelled()?;
                let Ok(entry) = entry else { continue };
                if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                    continue;
                }
                let path = entry.path().join("CURRENT");
                if env
                    .path_scope()
                    .is_some_and(|scope| !scope.mentions(&entry.path()))
                {
                    continue;
                }
                let Ok((sid, _)) = roots::identity(&env.provider_roots, &path) else {
                    continue;
                };
                let (stamp, recency) = if path.exists() {
                    crate::file_stamp_and_modified(&path)?
                } else {
                    ("missing-CURRENT".into(), None)
                };
                // Deltas change the active journal without rewriting CURRENT.
                // Inspect only the bounded pointer and its one active segment;
                // scanning all retained generations/journals would be unbounded.
                let active_journal = storage::bounded_read(
                    &AcquisitionContext::new(ID, &env.provider_roots),
                    &path,
                    16 * 1024,
                )
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|pointer| pointer["snapshot_sequence"].as_u64())
                .and_then(|seq| seq.checked_add(1))
                .map(|seq| entry.path().join(format!("journal/{seq:016}.jsonl")))
                .filter(|active| {
                    AcquisitionContext::new(ID, &env.provider_roots)
                        .validate(active)
                        .is_ok()
                })
                .and_then(|active| crate::file_stamp_and_modified(&active).ok());
                let journal_stamp = active_journal
                    .as_ref()
                    .map(|(stamp, _)| stamp.clone())
                    .unwrap_or_default();
                let recency = recency.max(active_journal.and_then(|(_, modified)| modified));
                found.push(Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: Some(sid),
                    recency_hint_ms: recency,
                    stamp: format!("{stamp}:{journal_stamp}"),
                });
            }
        }
        found.sort_by(|a, b| {
            b.recency_hint_ms
                .cmp(&a.recency_hint_ms)
                .then_with(|| a.locator.cmp(&b.locator))
        });
        found.dedup_by(|a, b| a.locator == b.locator);
        if let Some(limit) = limit {
            found.truncate(limit);
        }
        Ok(found)
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
        let path = Path::new(&candidate.locator);
        let (sid, native) = roots::identity(scan.provider_roots, path)?;
        let context = AcquisitionContext::new(ID, scan.provider_roots);
        let bytes = storage::bounded_read(&context, path, 16 * 1024)?;
        scan.note_open();
        scan.note_bytes(bytes.len() as u64);
        let current: Value = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(current["session_id"] == native, "VIBE_ID_MISMATCH");
        anyhow::ensure!(
            current["store_format"] == "mistral.vibe.unified-session-store/v1",
            "VIBE_UNSUPPORTED_FORMAT"
        );
        anyhow::ensure!(
            current["store_format_minor"].as_u64() == Some(9),
            "VIBE_UNSUPPORTED_FORMAT_MINOR"
        );
        if let Some(conn) = conn {
            normalize::check_identity(conn, &sid, path)?;
        }
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid,
            raw_path: Some(candidate.locator.clone()),
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
    let captured = storage::capture(path, context)?;
    anyhow::ensure!(
        captured.payload["sid"] == options.session_id,
        "VIBE_ID_MISMATCH"
    );
    Ok(captured)
}

fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let captured = ctx.provider_session.context("missing Vibe acquisition")?;
    anyhow::ensure!(
        captured.payload["partial"] != true,
        "VIBE_PARTIAL_JOURNAL: incomplete final record; prior evidence retained"
    );
    normalize::write(
        ctx.conn,
        &captured.payload,
        ctx.path.context("missing Vibe native path")?,
    )?;
    Ok((
        IngestOutcome {
            records: captured.records,
            ..Default::default()
        },
        Vec::new(),
        None,
    ))
}

fn sweep(ctx: SweepContext<'_>) -> Result<SweepResult> {
    let env = DiscoveryEnv::with_provider_roots(ctx.conn, ctx.roots.clone())
        .with_path_scope(ctx.paths.cloned());
    let context = AcquisitionContext::new(ID, ctx.roots);
    let mut result = SweepResult::default();
    for candidate in Provider.enumerate(&env, None)? {
        crate::ingest::check_capture_cancelled()?;
        let path = Path::new(&candidate.locator);
        let run = || -> Result<usize> {
            let captured = storage::capture(path, &context)?;
            anyhow::ensure!(captured.payload["partial"] != true, "VIBE_PARTIAL_JOURNAL");
            let tx = ctx.conn.unchecked_transaction()?;
            let written = normalize::write(&tx, &captured.payload, path)?;
            tx.commit()?;
            Ok(written)
        };
        match run() {
            Ok(written) => result.inserted += written,
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("Vibe {}: {error:#}", candidate.locator);
                result.unread += 1;
            }
        }
    }
    if ctx.paths.is_none() && result.unread == 0 {
        retire_deleted_sessions(ctx.conn, ctx.roots)?;
    }
    Ok(result)
}

/// Native `UnifiedSessionStore.delete` removes the entire session directory.
/// An absent CURRENT while generations remain may be a publication window or
/// damaged store, so it is not deletion evidence.
fn retire_deleted_sessions(conn: &Connection, roots: &ProviderRoots) -> Result<()> {
    let context = AcquisitionContext::new(ID, roots);
    let mut statement = conn.prepare(
        "SELECT DISTINCT locator FROM (\
         SELECT raw_path AS locator FROM sessions WHERE source=?1 AND raw_path IS NOT NULL \
         UNION SELECT raw_locator AS locator FROM session_observations WHERE source=?1 AND location='local' AND raw_locator IS NOT NULL \
         UNION SELECT raw_locator AS locator FROM session_presences WHERE source=?1 AND location='local' AND raw_locator IS NOT NULL)",
    )?;
    let locators = statement
        .query_map([ID], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for locator in locators {
        crate::ingest::check_capture_cancelled()?;
        let path = Path::new(&locator);
        if roots::identity(roots, path).is_err() {
            continue;
        }
        let Some(directory) = path.parent() else {
            continue;
        };
        let Some(root) = directory.parent() else {
            continue;
        };
        if !root.is_dir() {
            continue;
        }
        context.validate(root)?;
        // A missing pointer inside an extant store is not native delete.
        if std::fs::symlink_metadata(directory).is_ok() {
            continue;
        }
        std::fs::read_dir(root)?;
        if !directory.try_exists()? {
            let tx = conn.unchecked_transaction()?;
            let mut owners = tx.prepare("SELECT session_id FROM (\
                SELECT session_id FROM sessions WHERE source=?1 AND raw_path=?2 \
                UNION SELECT session_id FROM session_observations WHERE source=?1 AND location='local' AND raw_locator=?2 \
                UNION SELECT session_id FROM session_presences WHERE source=?1 AND location='local' AND raw_locator=?2)")?;
            let ids = owners
                .query_map(rusqlite::params![ID, locator], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            drop(owners);
            for sid in ids {
                crate::providers::file_history::retire_local_session(&tx, ID, &sid)?;
            }
            tx.commit()?;
        }
    }
    Ok(())
}
