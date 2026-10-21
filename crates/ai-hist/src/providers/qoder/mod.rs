//! Passive global Qoder CLI 1.1.x JSONL history. No agent process, authentication or model access.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;
use std::io::{Read, Seek, SeekFrom};
mod format;
mod normalize;
#[cfg(test)]
mod tests;
const ID: &str = "qoder";
const GENERATION: &str = "qoder_cli_1_1_v1";
const HEAD_LIMIT: u64 = 64 * 1024;
const OPENING_RECORD_LIMIT: usize = 8 * 1024 * 1024;
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
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: GENERATION,
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
fn expand(
    home: &Path,
    value: &str,
    cwd: impl FnOnce() -> std::io::Result<PathBuf>,
) -> std::io::Result<PathBuf> {
    let path = if value == "~" {
        home.to_path_buf()
    } else if let Some(rest) = value.strip_prefix("~/") {
        home.join(rest)
    } else {
        PathBuf::from(value)
    };
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(cwd()?.join(path))
    }
}
fn resolve_roots(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let value = use_env
        .then(|| std::env::var("QODER_CONFIG_DIR").ok())
        .flatten()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "~/.qoder".into());
    let config = match expand(home, &value, std::env::current_dir) {
        Ok(config) => config,
        Err(error) => {
            return ResolvedProviderRoots {
                error: Some(format!(
                    "QODER_INVALID_ROOT: cannot resolve current directory: {error}"
                )),
                ..Default::default()
            }
        }
    };
    vec![config.join("projects")].into()
}

fn prefix(
    context: &AcquisitionContext<'_>,
    path: &Path,
    scan: &ScanEnv<'_>,
) -> Result<(Vec<u8>, std::fs::File)> {
    context.validate(path)?;
    let mut bytes = Vec::new();
    let mut file = std::fs::File::open(path)?;
    scan.note_open();
    let read = (&mut file).take(HEAD_LIMIT).read_to_end(&mut bytes);
    scan.note_bytes(bytes.len() as u64);
    read?;
    // A single native user message can exceed the normal shallow prefix.
    // Read only through its first terminated record, with an explicit cap.
    while !bytes.contains(&b'\n') && bytes.len() < OPENING_RECORD_LIMIT {
        let mut chunk = [0_u8; 8192];
        let remaining = chunk.len().min(OPENING_RECORD_LIMIT - bytes.len());
        let count = file.read(&mut chunk[..remaining])?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        scan.note_bytes(count as u64);
    }
    anyhow::ensure!(
        bytes.contains(&b'\n') || bytes.len() < OPENING_RECORD_LIMIT,
        "QODER_OVERSIZED_OPENING_RECORD: no terminated opening record within 8 MiB"
    );
    Ok((bytes, file))
}
fn tail_metadata(
    file: &mut std::fs::File,
    head_len: usize,
    head_ends_line: bool,
    sid: &str,
) -> Result<(Option<i64>, Option<String>, usize, u64)> {
    let size = file.metadata()?.len();
    if size <= head_len as u64 {
        return Ok((None, None, 0, 0));
    }
    let start = size.saturating_sub(HEAD_LIMIT).max(head_len as u64);
    file.seek(SeekFrom::Start(start))?;
    let mut tail = Vec::new();
    (&mut *file).take(HEAD_LIMIT).read_to_end(&mut tail)?;
    let offset = if start == head_len as u64 && head_ends_line {
        0
    } else {
        tail.iter()
            .position(|b| *b == b'\n')
            .map_or(tail.len(), |p| p + 1)
    };
    let (last, title, records) = metadata_rows(&tail, offset, sid)?;
    Ok((last, title, records, tail.len() as u64))
}
fn metadata_rows(
    bytes: &[u8],
    offset: usize,
    sid: &str,
) -> Result<(Option<i64>, Option<String>, usize)> {
    let end = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |p| p + 1);
    let mut last = None;
    let mut title = None;
    let mut records = 0;
    if offset < end {
        for line in bytes[offset..end]
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
        {
            let entry: Value = serde_json::from_slice(line)
                .context("QODER_INVALID_RECORD: malformed complete shallow tail")?;
            anyhow::ensure!(
                format::string(&entry, "sessionId")? == sid,
                "QODER_INVALID_RECORD: mixed session IDs"
            );
            if let Some(timestamp) = format::time(&entry)? {
                last = Some(last.map_or(timestamp, |value: i64| value.max(timestamp)));
            }
            if entry["type"] == "custom-title" {
                title = entry["customTitle"].as_str().map(str::to_owned);
            }
            records += 1;
        }
    }
    Ok((last, title, records))
}
fn project_candidates(
    project: &Path,
    scope: Option<&crate::ingest::path_scope::PathScope>,
    out: &mut Vec<Candidate>,
) -> Result<()> {
    for file in std::fs::read_dir(project)? {
        crate::ingest::check_capture_cancelled()?;
        let file = file?;
        if !file.file_type()?.is_file() || file.path().extension().is_none_or(|ext| ext != "jsonl")
        {
            continue;
        }
        let path = file.path();
        if scope.is_some_and(|scope| !scope.mentions(&path)) {
            continue;
        }
        let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&path)?;
        out.push(Candidate {
            source: ID,
            locator: path.to_string_lossy().into_owned(),
            session_id: None,
            recency_hint_ms,
            stamp,
        });
    }
    Ok(())
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
        env.provider_roots.ensure_provider_roots(ID)?;
        for root in env.provider_roots.for_provider(ID) {
            if env.path_scope().is_some_and(|s| !s.mentions(root)) {
                continue;
            }
            match std::fs::metadata(root) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                result => {
                    result.with_context(|| format!("Qoder root {}", root.display()))?;
                }
            }
            for project in std::fs::read_dir(root)? {
                crate::ingest::check_capture_cancelled()?;
                let project = project?;
                if env
                    .path_scope()
                    .is_some_and(|scope| !scope.mentions(&project.path()))
                {
                    continue;
                }
                if !project.file_type()?.is_dir() {
                    continue;
                }
                project_candidates(&project.path(), env.path_scope(), &mut out)?;
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
        let (bytes, mut file) = prefix(
            &AcquisitionContext::new(ID, scan.provider_roots),
            Path::new(&c.locator),
            scan,
        )?;
        let entries = format::parse(&bytes, true)?;
        let header = &entries[0];
        let sid = format::string(header, "sessionId")?;
        let (head_last, head_title, head_records) = metadata_rows(&bytes, 0, sid)?;
        scan.note_records(head_records as u64);
        let (tail_last, tail_title, tail_records, tail_bytes) =
            tail_metadata(&mut file, bytes.len(), bytes.last() == Some(&b'\n'), sid)?;
        if tail_bytes > 0 {
            scan.note_bytes(tail_bytes);
            scan.note_records(tail_records as u64);
        }
        if let Some(conn) = conn {
            normalize::check_identity(conn, sid, Path::new(&c.locator))?;
        }
        let first_prompt = format::first_prompt(&entries);
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid.into(),
            cwd: header["cwd"].as_str().map(str::to_owned),
            first_prompt,
            first_activity_ms: format::time(header)?,
            last_activity_ms: head_last.into_iter().chain(tail_last).max(),
            title: tail_title.or(head_title),
            raw_path: Some(c.locator.clone()),
            source_stamp: Some(c.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}
fn snapshot(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let bytes = context.read(path)?;
    let entries = format::parse(&bytes, false)?;
    let records = entries.len() as i64;
    // Preserve the entire trailing fragment in the stamp, without normalizing it.
    let tail = bytes.rsplit(|b| *b == b'\n').next().unwrap_or_default();
    Ok(AcquiredSession::new(
        serde_json::json!({"entries":entries,"tail":tail}),
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
        result.payload["entries"][0]["sessionId"].as_str() == Some(&options.session_id),
        "SESSION_SOURCE_MISMATCH: Qoder native identity changed"
    );
    Ok(result)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let snapshot = ctx
        .provider_session
        .context("Qoder acquisition snapshot missing")?;
    normalize::write(
        ctx.conn,
        &snapshot.payload,
        ctx.path.context("Qoder path missing")?,
    )?;
    let diagnostics = if snapshot.payload["tail"]
        .as_array()
        .is_some_and(|a| !a.is_empty())
    {
        vec![crate::hydrate::HydrationDiagnostic {
            code: "QODER_PARTIAL_TAIL".into(),
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
fn cache_hit(
    conn: &Connection,
    candidate: &Candidate,
    previous: &Value,
    repairs: &crate::ingest::SweepRepairs,
) -> Result<bool> {
    let Some(sid) = previous["sid"].as_str() else {
        return Ok(false);
    };
    if previous["stamp"].as_str() != Some(&candidate.stamp)
        || previous["generation"].as_str() != Some(GENERATION)
        || repairs.contains(ID, sid)
    {
        return Ok(false);
    }
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE source='qoder' AND session_id=?1)",
        [sid],
        |row| row.get(0),
    )?)
}
fn sweep(ctx: SweepContext<'_>) -> Result<SweepResult> {
    let env = DiscoveryEnv::with_provider_roots(ctx.conn, ctx.roots.clone())
        .with_path_scope(ctx.paths.cloned());
    let mut result = SweepResult::default();
    let cache_key = "qoder_file_stamps";
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
        if let Some(previous) = cache.get(&candidate.locator) {
            if cache_hit(ctx.conn, &candidate, previous, ctx.repairs)? {
                continue;
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
                format::string(&snapshot.payload["entries"][0], "sessionId")?.to_owned(),
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
                eprintln!("Qoder {}: {error:#}", path.display());
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
