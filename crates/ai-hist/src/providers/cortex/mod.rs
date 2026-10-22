//! Passive Cortex Code CLI 1.2.18 snapshot and JSONL history. No agent process, authentication or model access.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;
use std::io::{Read, Seek, SeekFrom};
mod format;
mod normalize;
mod sweep;
#[cfg(test)]
mod tests;
const ID: &str = "cortex";
const GENERATION: &str = "cortex_cli_1_2_18_v1";
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
    sweep: sweep::run,
    resolve_roots,
    watch_roots: |r| {
        r.for_provider(ID)
            .iter()
            .cloned()
            .map(WatchRoot::tree)
            .collect()
    },
});
fn expand(home: &Path, value: &str) -> Result<PathBuf> {
    let path = if value == "~" {
        home.to_path_buf()
    } else if let Some(rest) = value.strip_prefix("~/") {
        home.join(rest)
    } else {
        PathBuf::from(value)
    };
    Ok(if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    })
}
fn resolve_roots(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let cortex_home = use_env
        .then(|| std::env::var("CORTEX_HOME").ok())
        .flatten()
        .filter(|v| !v.is_empty());
    let snowflake_home = use_env
        .then(|| std::env::var("SNOWFLAKE_HOME").ok())
        .flatten()
        .filter(|v| !v.is_empty());
    let resolved = (|| -> Result<PathBuf> {
        if let Some(value) = cortex_home {
            Ok(expand(home, &value)?.join("conversations"))
        } else if let Some(value) = snowflake_home {
            Ok(expand(home, &value)?.join("cortex/conversations"))
        } else {
            Ok(home.join(".snowflake/cortex/conversations"))
        }
    })();
    match resolved {
        Ok(root) => vec![root].into(),
        Err(error) => ResolvedProviderRoots {
            error: Some(format!("CORTEX_INVALID_ROOT: {error}")),
            ..Default::default()
        },
    }
}
fn session_id(path: &Path) -> Result<&str> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty() && !s.contains('/'))
        .context("CORTEX_INVALID_METADATA: filename ID")
}
fn sidecar(path: &Path) -> Result<PathBuf> {
    Ok(path.with_file_name(format!("{}.history.jsonl", session_id(path)?)))
}
fn exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}
fn read_metadata(context: &AcquisitionContext<'_>, path: &Path) -> Result<Vec<u8>> {
    context.validate(path)?;
    const LIMIT: u64 = 8 * 1024 * 1024;
    anyhow::ensure!(
        std::fs::metadata(path)?.len() <= LIMIT,
        "CORTEX_OVERSIZED_METADATA"
    );
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() as u64 <= LIMIT, "CORTEX_OVERSIZED_METADATA");
    Ok(bytes)
}
fn compound_stamp(path: &Path) -> Result<(String, Option<i64>)> {
    let (meta, recency) = crate::file_stamp_and_modified(path)?;
    let side = sidecar(path)?;
    let (tail, tail_recency) = match std::fs::metadata(&side) {
        Ok(_) => crate::file_stamp_and_modified(&side)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => ("absent".into(), None),
        Err(e) => return Err(e.into()),
    };
    Ok((
        format!("{meta}|{tail}"),
        recency.into_iter().chain(tail_recency).max(),
    ))
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
    (&mut file).take(HEAD_LIMIT).read_to_end(&mut bytes)?;
    scan.note_bytes(bytes.len() as u64);
    if bytes.is_empty() && file.metadata()?.len() == 0 {
        return Ok((bytes, file));
    }
    while !bytes.contains(&b'\n') && bytes.len() < OPENING_RECORD_LIMIT {
        let mut chunk = [0u8; 8192];
        let remaining = chunk.len().min(OPENING_RECORD_LIMIT - bytes.len());
        let n = file.read(&mut chunk[..remaining])?;
        if n == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..n]);
        scan.note_bytes(n as u64);
    }
    anyhow::ensure!(
        bytes.contains(&b'\n'),
        "CORTEX_OVERSIZED_OPENING_RECORD: no complete record within 8 MiB"
    );
    Ok((bytes, file))
}
fn tail_metadata(
    file: &mut std::fs::File,
    head_len: usize,
    head_ends_line: bool,
    meta: &Value,
) -> Result<(Option<i64>, usize, u64)> {
    let size = file.metadata()?.len();
    if size <= head_len as u64 {
        return Ok((None, 0, 0));
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
    if offset >= tail.len() {
        return Ok((None, 0, tail.len() as u64));
    }
    let (entries, _) = format::history(meta, Some(&tail[offset..]))?;
    let last = entries
        .iter()
        .filter_map(|e| format::time(e).ok().flatten())
        .max();
    Ok((last, entries.len(), tail.len() as u64))
}
fn project_candidates(
    root: &Path,
    scope: Option<&crate::ingest::path_scope::PathScope>,
    out: &mut Vec<Candidate>,
) -> Result<()> {
    for file in std::fs::read_dir(root)? {
        crate::ingest::check_capture_cancelled()?;
        let file = file?;
        if !file.file_type()?.is_file()
            || file.path().extension().is_none_or(|ext| ext != "json")
            || file.file_name().to_string_lossy().contains(".back.")
        {
            continue;
        }
        let path = file.path();
        if scope
            .is_some_and(|s| !s.mentions(&path) && !sidecar(&path).is_ok_and(|p| s.mentions(&p)))
        {
            continue;
        }
        let (stamp, recency_hint_ms) = match compound_stamp(&path) {
            Ok(stamp) => stamp,
            Err(error) => {
                eprintln!("Cortex discovery skipped {}: {error:#}", path.display());
                continue;
            }
        };
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
    fn enumerate(&self, env: &DiscoveryEnv<'_>, limit: Option<usize>) -> Result<Vec<Candidate>> {
        let mut out = Vec::new();
        env.provider_roots.ensure_provider_roots(ID)?;
        for root in env.provider_roots.for_provider(ID) {
            if env.path_scope().is_some_and(|s| !s.mentions(root)) {
                continue;
            }
            match std::fs::metadata(root) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                result => {
                    result.with_context(|| format!("Cortex root {}", root.display()))?;
                }
            }
            project_candidates(root, env.path_scope(), &mut out)?;
        }
        out.sort_by(|a, b| {
            b.recency_hint_ms
                .cmp(&a.recency_hint_ms)
                .then_with(|| a.locator.cmp(&b.locator))
        });
        out.dedup_by(|a, b| a.locator == b.locator);
        if let Some(limit) = limit {
            out.truncate(limit);
        }
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
        let path = Path::new(&c.locator);
        let context = AcquisitionContext::new(ID, scan.provider_roots);
        context.validate(path)?;
        let meta_bytes = read_metadata(&context, path)?;
        scan.note_open();
        scan.note_bytes(meta_bytes.len() as u64);
        let meta = format::metadata(&meta_bytes, session_id(path)?)?;
        let sid = format::string(&meta, "session_id")?;
        let side = sidecar(path)?;
        let (first, last, first_prompt) = if exists(&side)? {
            let (head, mut file) = prefix(&context, &side, scan)?;
            let (entries, _) = format::history(&meta, Some(&head))?;
            scan.note_records(entries.len() as u64);
            let (tail_last, tail_records, tail_bytes) =
                tail_metadata(&mut file, head.len(), head.last() == Some(&b'\n'), &meta)?;
            scan.note_bytes(tail_bytes);
            scan.note_records(tail_records as u64);
            let first = entries
                .iter()
                .filter_map(|e| format::time(e).ok().flatten())
                .min()
                .or(Some(format::created_at(&meta)?));
            let last = entries
                .iter()
                .filter_map(|e| format::time(e).ok().flatten())
                .max()
                .into_iter()
                .chain(tail_last)
                .max()
                .or(Some(format::created_at(&meta)?));
            (first, last, format::first_prompt(&entries))
        } else {
            let (entries, _) = format::history(&meta, None)?;
            scan.note_records(entries.len() as u64);
            let first = entries
                .iter()
                .filter_map(|e| format::time(e).ok().flatten())
                .min()
                .or(Some(format::created_at(&meta)?));
            let last = entries
                .iter()
                .filter_map(|e| format::time(e).ok().flatten())
                .max()
                .or(Some(format::created_at(&meta)?));
            (first, last, format::first_prompt(&entries))
        };
        if let Some(conn) = conn {
            normalize::check_identity(conn, sid, path)?;
        }
        anyhow::ensure!(
            compound_stamp(path)?.0 == c.stamp,
            "CORTEX_CHANGED_DURING_CAPTURE: shallow metadata and sidecar changed; retry"
        );
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid.into(),
            cwd: meta["working_directory"].as_str().map(str::to_owned),
            first_prompt,
            first_activity_ms: first,
            last_activity_ms: last,
            title: meta["title"].as_str().map(str::to_owned),
            raw_path: Some(c.locator.clone()),
            source_stamp: Some(c.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}
fn snapshot(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let before = compound_stamp(path)?.0;
    let meta_bytes = read_metadata(context, path)?;
    let meta = format::metadata(&meta_bytes, session_id(path)?)?;
    let side = sidecar(path)?;
    let side_bytes = if exists(&side)? {
        Some(context.read(&side)?)
    } else {
        None
    };
    anyhow::ensure!(
        before == compound_stamp(path)?.0,
        "CORTEX_CHANGED_DURING_CAPTURE: snapshot and sidecar changed; retry"
    );
    let (entries, partial) = format::history(&meta, side_bytes.as_deref())?;
    let records = entries.len() as i64;
    let tail = if partial {
        side_bytes
            .as_deref()
            .unwrap_or_default()
            .rsplit(|b| *b == b'\n')
            .next()
            .unwrap_or_default()
            .to_vec()
    } else {
        Vec::new()
    };
    let mut paths = vec![path.to_path_buf()];
    if side_bytes.is_some() {
        paths.push(side);
    }
    Ok(AcquiredSession::new(
        serde_json::json!({"entries":entries,"tail":tail,"metadata":meta}),
        (meta_bytes.len() + side_bytes.as_ref().map_or(0, Vec::len)) as i64,
        records,
        paths,
    ))
}
fn acquire(
    options: &HydrateSessionOptions,
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<AcquiredSession> {
    let result = snapshot(path, context)?;
    anyhow::ensure!(
        result.payload["metadata"]["session_id"].as_str() == Some(&options.session_id),
        "SESSION_SOURCE_MISMATCH: Cortex native identity changed"
    );
    Ok(result)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let snapshot = ctx
        .provider_session
        .context("Cortex acquisition snapshot missing")?;
    normalize::write(
        ctx.conn,
        &snapshot.payload,
        ctx.path.context("Cortex path missing")?,
    )?;
    let diagnostics = if snapshot.payload["tail"]
        .as_array()
        .is_some_and(|a| !a.is_empty())
    {
        vec![crate::hydrate::HydrationDiagnostic {
            code: "CORTEX_PARTIAL_TAIL".into(),
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
