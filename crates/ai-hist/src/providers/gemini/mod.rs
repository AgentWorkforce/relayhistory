//! Passive Gemini CLI journal and legacy JSON history. No agent process, configuration or model access.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;
use sha2::{Digest, Sha256};
use std::io::Read;
mod format;
mod normalize;
mod storage;
#[cfg(test)]
mod tests;
mod usage;
const ID: &str = "gemini";
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
    generation: "gemini_journal_v4",
    acquire,
    sweep,
    resolve_roots,
    watch_roots: |r| {
        r.for_provider(ID)
            .iter()
            .cloned()
            .map(WatchRoot::tree)
            .collect()
    },
});
fn resolve_roots(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let env = |key| {
        use_env
            .then(|| std::env::var(key).ok())
            .flatten()
            .filter(|s| !s.is_empty())
    };
    roots_from_values(
        home,
        env("GEMINI_CLI_HOME").as_deref(),
        env("SANDBOX").as_deref(),
    )
}
fn roots_from_values(
    home: &Path,
    base: Option<&str>,
    sandbox: Option<&str>,
) -> ResolvedProviderRoots {
    let base_path = base.map(PathBuf::from).unwrap_or_else(|| home.to_owned());
    let runtime = if sandbox == Some("sandbox-exec") {
        base_path.join(".cache/.gemini")
    } else {
        base_path.join(".gemini")
    };
    let root = runtime.join("tmp");
    let mut resolved: ResolvedProviderRoots = vec![root.clone()].into();
    resolved.default_root = base.is_none().then_some(root);
    resolved
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
        storage::enumerate(env)
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
        let bytes = prefix(
            &AcquisitionContext::new(ID, scan.provider_roots),
            Path::new(&c.locator),
        )?;
        scan.note_open();
        scan.note_bytes(bytes.len() as u64);
        let header = format::header(&bytes, path_is_legacy(Path::new(&c.locator)))?;
        scan.note_records(1);
        let native = format::string(&header, "sessionId")?;
        let sid = storage::identity(scan.provider_roots, Path::new(&c.locator), native)?;
        if storage::superseded(
            &AcquisitionContext::new(ID, scan.provider_roots),
            Path::new(&c.locator),
            native,
            format::string(&header, "projectHash")?,
        )? {
            return Ok(None);
        }
        if let Some(conn) = conn {
            normalize::check_identity(conn, &sid, Path::new(&c.locator))?;
        }
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid,
            first_activity_ms: Some(format::date(&header, "startTime")?),
            last_activity_ms: Some(storage::recent_time(
                scan,
                Path::new(&c.locator),
                &bytes,
                &header,
            )?),
            raw_path: Some(c.locator.clone()),
            source_stamp: Some(c.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}
fn path_is_legacy(path: &Path) -> bool {
    path.extension().is_some_and(|e| e == "json")
}
fn snapshot(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let primary = snapshot_file(path, context)?;
    let sibling = path.with_extension(if path_is_legacy(path) {
        "jsonl"
    } else {
        "json"
    });
    if !sibling.try_exists()? {
        return Ok(primary);
    }
    let secondary = snapshot_file(&sibling, context)?;
    let (mut journal, legacy) = if path_is_legacy(path) {
        (secondary, primary)
    } else {
        (primary, secondary)
    };
    anyhow::ensure!(
        journal.payload["conversation"]["sessionId"] == legacy.payload["conversation"]["sessionId"]
            && journal.payload["conversation"]["projectHash"]
                == legacy.payload["conversation"]["projectHash"],
        "GEMINI_ID_COLLISION: migration sibling metadata mismatch"
    );
    journal.payload["legacyBytesSha256"] = legacy.payload["bytesSha256"].clone();
    journal.payload["legacyPath"] = legacy.read_paths[0].to_string_lossy().to_string().into();
    journal.payload["migrationJournal"] =
        journal.read_paths[0].to_string_lossy().to_string().into();
    let bytes = journal.bytes + legacy.bytes;
    let mut paths = journal.read_paths;
    paths.extend(legacy.read_paths);
    paths.sort();
    Ok(AcquiredSession::new(
        journal.payload,
        bytes,
        journal.records,
        paths,
    ))
}
fn snapshot_file(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let bytes = context.read(path)?;
    let mut payload = format::parse(&bytes, path_is_legacy(path))?;
    let native = format::string(&payload["conversation"], "sessionId")?;
    let sid = storage::identity(context.roots, path, native)?;
    payload["ledgerId"] = sid.into();
    let records = payload["records"].as_array().map_or(0, Vec::len) as i64;
    // Exact bytes, including whitespace and the incomplete tail, enter the stamp.
    payload["bytesSha256"] = format!("{:x}", Sha256::digest(&bytes)).into();
    Ok(AcquiredSession::new(
        payload,
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
    let result = snapshot(path, context)?;
    anyhow::ensure!(
        result.payload["ledgerId"].as_str() == Some(&options.session_id),
        "SESSION_SOURCE_MISMATCH: Gemini native identity changed"
    );
    Ok(result)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let snapshot = ctx
        .provider_session
        .context("Gemini acquisition snapshot missing")?;
    normalize::write(
        ctx.conn,
        &snapshot.payload,
        ctx.path.context("Gemini path missing")?,
    )?;
    let diagnostics = if snapshot.payload["partial"].as_bool() == Some(true) {
        vec![crate::hydrate::HydrationDiagnostic {
            code: "GEMINI_PARTIAL_TAIL".into(),
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
    let cache_key = "gemini_file_stamps";
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
        let generation = ADAPTER.unwrap().generation;
        if cached(
            ctx.conn,
            cache.get(&candidate.locator),
            &candidate,
            generation,
            ctx.repairs,
        )? {
            continue;
        }
        let ingest = || -> Result<(usize, bool, String)> {
            let snapshot = snapshot(path, &AcquisitionContext::new(ID, ctx.roots))?;
            let tx = ctx.conn.unchecked_transaction()?;
            let inserted = normalize::write(&tx, &snapshot.payload, path)?;
            tx.commit()?;
            Ok((
                inserted,
                snapshot.payload["partial"].as_bool() == Some(true),
                format::string(&snapshot.payload, "ledgerId")?.to_owned(),
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
                eprintln!("Gemini {}: {error:#}", path.display());
                cache.remove(&candidate.locator);
                result.unread += 1;
            }
        }
    }
    if ctx.paths.is_none() && result.unread == 0 {
        crate::providers::file_history::retire_missing(ctx.conn, ID, ctx.roots)?;
        storage::retire_missing_legacy(ctx.conn, ctx.roots)?;
    }
    ctx.state.insert(cache_key.into(), Value::Object(cache));
    Ok(result)
}

fn cached(
    conn: &Connection,
    previous: Option<&Value>,
    candidate: &Candidate,
    generation: &str,
    repairs: &crate::ingest::SweepRepairs,
) -> Result<bool> {
    let Some(previous) = previous else {
        return Ok(false);
    };
    if previous["stamp"].as_str() != Some(&candidate.stamp)
        || previous["generation"].as_str() != Some(generation)
    {
        return Ok(false);
    }
    let Some(sid) = previous["sid"].as_str() else {
        return Ok(false);
    };
    if repairs.contains(ID, sid) {
        return Ok(false);
    }
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE source='gemini' AND session_id=?1)",
        [sid],
        |r| r.get(0),
    )?)
}
