//! Passive GitHub Copilot CLI history: the session journals under
//! `$COPILOT_HOME/session-state/<id>/events.jsonl` (default `~/.copilot`).
//! No agent process, configuration, authentication or model access.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::ingest::path_scope::{PathScope, RootWalk};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;
use std::io::{Read, Seek};
mod format;
mod normalize;
#[cfg(test)]
mod tests;
mod usage;
const ID: &str = "copilot";
const JOURNAL: &str = "events.jsonl";
const HEAD_LIMIT: u64 = 64 * 1024;
const TAIL_LIMIT: u64 = 16 * 1024;
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
const GENERATION: &str = "copilot_v1_1";
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: GENERATION,
    acquire,
    sweep,
    resolve_roots: |home, use_env| vec![resolve_roots(home, use_env)].into(),
    watch_roots: |r| {
        r.for_provider(ID)
            .iter()
            .cloned()
            .map(WatchRoot::tree)
            .collect()
    },
});

/// `COPILOT_HOME` replaces the whole configuration root.
fn resolve_roots(home: &Path, use_env: bool) -> PathBuf {
    super::resolve_root(home, use_env, "COPILOT_HOME", ".copilot").join("session-state")
}

/// Each session directory's journal, without walking checkpoints, file
/// snapshots or other per-session sidecars.
fn journals(root: &Path, scope: Option<&PathScope>) -> Result<Vec<PathBuf>> {
    let dirs = match PathScope::walk_of(scope, root) {
        RootWalk::Skip => return Ok(Vec::new()),
        RootWalk::Full => std::fs::read_dir(root)?
            .map(|entry| entry.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>()?,
        RootWalk::Partial(scope) => scope.children_under(root),
    };
    let mut out = Vec::new();
    for dir in dirs {
        crate::ingest::check_capture_cancelled()?;
        let journal = dir.join(JOURNAL);
        if journal.is_file() {
            out.push(journal);
        }
    }
    Ok(out)
}

fn read_window(context: &AcquisitionContext<'_>, path: &Path) -> Result<(Vec<u8>, Vec<u8>)> {
    context.validate(path)?;
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let mut head = Vec::new();
    (&mut file).take(HEAD_LIMIT).read_to_end(&mut head)?;
    let mut tail = Vec::new();
    if len > HEAD_LIMIT {
        file.seek(std::io::SeekFrom::Start(
            len.saturating_sub(TAIL_LIMIT).max(HEAD_LIMIT),
        ))?;
        file.take(TAIL_LIMIT).read_to_end(&mut tail)?;
    }
    Ok((head, tail))
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
            match std::fs::metadata(root) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                result => {
                    result.with_context(|| format!("Copilot root {}", root.display()))?;
                }
            }
            for path in journals(root, env.path_scope())
                .with_context(|| format!("Copilot root {}", root.display()))?
            {
                let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&path)?;
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
        // catalog write, including two journals in a fresh discovery run.
        crate::discover::ShallowReadAccess::Catalog
    }
    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        conn: Option<&Connection>,
        c: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let path = Path::new(&c.locator);
        let (head, tail) = read_window(&AcquisitionContext::new(ID, scan.provider_roots), path)?;
        scan.note_open();
        scan.note_bytes((head.len() + tail.len()) as u64);
        let entries = format::parse(&head, true)?;
        scan.note_records(entries.len() as u64);
        let header = &entries[0];
        let sid = format::session_id(header)?;
        if let Some(conn) = conn {
            normalize::check_identity(conn, sid, path)?;
        }
        let data = &header["data"];
        let head_last = entries
            .iter()
            .map(format::time)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max();
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid.into(),
            cwd: data["context"]["cwd"].as_str().map(str::to_owned),
            git_branch: data["context"]["branch"].as_str().map(str::to_owned),
            first_prompt: format::first_prompt(&entries),
            first_activity_ms: Some(format::time(header)?),
            // A journal that fits the head has no separate tail window.
            last_activity_ms: format::last_time(if tail.is_empty() { &head } else { &tail })
                .max(head_last),
            title: format::title(&entries).map(str::to_owned),
            models: catalog_models(conn, sid, format::models(&entries))?,
            agent_version: data["copilotVersion"].as_str().map(str::to_owned),
            raw_path: Some(c.locator.clone()),
            source_stamp: Some(c.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}

/// The bounded head's models, after every model a full ingest of this
/// session already recorded: a journal only appends, so a later model the
/// head cannot see stays in the catalog.
fn catalog_models(conn: Option<&Connection>, sid: &str, head: Vec<String>) -> Result<Vec<String>> {
    use rusqlite::OptionalExtension;
    let stored: Option<String> = match conn {
        Some(conn) => conn
            .query_row(
                "SELECT models_json FROM sessions WHERE source='copilot' AND session_id=?1 AND discovery_state='full'",
                [sid],
                |r| r.get(0),
            )
            .optional()?
            .flatten(),
        None => None,
    };
    let mut models: Vec<String> = stored
        .and_then(|json| serde_json::from_str(&json).ok())
        .unwrap_or_default();
    for model in head {
        if !models.contains(&model) {
            models.push(model);
        }
    }
    Ok(models)
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
        result.payload["entries"][0]["data"]["sessionId"].as_str() == Some(&options.session_id),
        "SESSION_SOURCE_MISMATCH: Copilot native identity changed"
    );
    Ok(result)
}

fn partial(payload: &Value) -> bool {
    payload["tail"].as_array().is_some_and(|a| !a.is_empty())
}

fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let snapshot = ctx
        .provider_session
        .context("Copilot acquisition snapshot missing")?;
    normalize::write(
        ctx.conn,
        &snapshot.payload,
        ctx.path.context("Copilot path missing")?,
    )?;
    let diagnostics = if partial(&snapshot.payload) {
        vec![crate::hydrate::HydrationDiagnostic {
            code: "COPILOT_PARTIAL_TAIL".into(),
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

/// Whether the cached stamp proves this journal is already in the store.
fn unchanged(ctx: &SweepContext<'_>, previous: Option<&Value>, stamp: &str) -> Result<bool> {
    let Some(previous) = previous else {
        return Ok(false);
    };
    let generation = GENERATION;
    let Some(sid) = previous["sid"].as_str() else {
        return Ok(false);
    };
    if previous["stamp"].as_str() != Some(stamp)
        || previous["generation"].as_str() != Some(generation)
        || ctx.repairs.contains(ID, sid)
    {
        return Ok(false);
    }
    Ok(ctx.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE source='copilot' AND session_id=?1)",
        [sid],
        |r| r.get(0),
    )?)
}

fn sweep_one(ctx: &SweepContext<'_>, path: &Path) -> Result<(usize, bool, String)> {
    let snapshot = snapshot(path, &AcquisitionContext::new(ID, ctx.roots))?;
    let tx = ctx.conn.unchecked_transaction()?;
    let inserted = normalize::write(&tx, &snapshot.payload, path)?;
    tx.commit()?;
    Ok((
        inserted,
        partial(&snapshot.payload),
        format::session_id(&snapshot.payload["entries"][0])?.to_owned(),
    ))
}

fn sweep(ctx: SweepContext<'_>) -> Result<SweepResult> {
    let env = DiscoveryEnv::with_provider_roots(ctx.conn, ctx.roots.clone())
        .with_path_scope(ctx.paths.cloned());
    let mut result = SweepResult::default();
    let cache_key = "copilot_file_stamps";
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
    let generation = GENERATION;
    for candidate in candidates {
        crate::ingest::check_capture_cancelled()?;
        if unchanged(&ctx, cache.get(&candidate.locator), &candidate.stamp)? {
            continue;
        }
        let path = Path::new(&candidate.locator);
        match sweep_one(&ctx, path) {
            Ok((n, partial, sid)) => {
                if partial {
                    cache.remove(&candidate.locator);
                } else {
                    cache.insert(
                        candidate.locator.clone(),
                        serde_json::json!({"stamp": candidate.stamp, "generation": generation, "sid": sid}),
                    );
                }
                result.inserted += n;
                result.unread += u64::from(partial);
            }
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("Copilot {}: {error:#}", path.display());
                cache.remove(&candidate.locator);
                result.unread += 1;
            }
        }
    }
    if ctx.paths.is_none() && result.unread == 0 {
        file_history::retire_missing(ctx.conn, ID, ctx.roots)?;
    }
    ctx.state.insert(cache_key.into(), Value::Object(cache));
    Ok(result)
}
