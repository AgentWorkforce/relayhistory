//! Passive Cline core v1 messages artifact reader. Never launches Cline.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;
use std::io::Read;
mod format;
mod normalize;
#[cfg(test)]
mod tests;
const ID: &str = "cline";
const GENERATION: &str = "cline_core_messages_v1_1";
const MAX_SNAPSHOT: u64 = 32 * 1024 * 1024;
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

fn expand(home: &Path, value: &str) -> std::io::Result<PathBuf> {
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
        Ok(std::env::current_dir()?.join(path))
    }
}
fn roots_from_values(
    home: &Path,
    sessions: Option<&str>,
    data: Option<&str>,
    config: Option<&str>,
) -> ResolvedProviderRoots {
    let value = if let Some(sessions) = sessions.filter(|s| !s.trim().is_empty()) {
        sessions.to_owned()
    } else if let Some(data) = data.filter(|s| !s.trim().is_empty()) {
        format!("{data}/sessions")
    } else if let Some(config) = config.filter(|s| !s.trim().is_empty()) {
        format!("{config}/data/sessions")
    } else {
        home.join(".cline/data/sessions")
            .to_string_lossy()
            .into_owned()
    };
    match expand(home, &value) {
        Ok(path) => vec![path].into(),
        Err(error) => ResolvedProviderRoots {
            error: Some(format!("CLINE_INVALID_ROOT: {error}")),
            ..Default::default()
        },
    }
}
fn resolve_roots(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let sessions = use_env
        .then(|| std::env::var("CLINE_SESSION_DATA_DIR").ok())
        .flatten();
    let data = use_env
        .then(|| std::env::var("CLINE_DATA_DIR").ok())
        .flatten();
    let config = use_env.then(|| std::env::var("CLINE_DIR").ok()).flatten();
    roots_from_values(
        home,
        sessions.as_deref(),
        data.as_deref(),
        config.as_deref(),
    )
}

fn artifact(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    context.validate(path)?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(MAX_SNAPSHOT + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() as u64 <= MAX_SNAPSHOT,
        "CLINE_SNAPSHOT_TOO_LARGE: 32 MiB limit"
    );
    let sid = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .context("CLINE_INVALID_PATH: no session directory")?;
    anyhow::ensure!(
        path.file_name().and_then(|s| s.to_str()) == Some(&format!("{sid}.messages.json")),
        "CLINE_INVALID_PATH: artifact name does not match session directory"
    );
    let payload = format::parse(&bytes, sid)?;
    let records = payload["messages"].as_array().map_or(0, Vec::len) as i64;
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
    let snapshot = artifact(path, context)?;
    anyhow::ensure!(
        snapshot.payload["sessionId"].as_str() == Some(&options.session_id),
        "SESSION_SOURCE_MISMATCH: Cline native identity changed"
    );
    Ok(snapshot)
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
        env.provider_roots.ensure_provider_roots(ID)?;
        let mut out = Vec::new();
        for root in env.provider_roots.for_provider(ID) {
            if env.path_scope().is_some_and(|s| !s.mentions(root)) {
                continue;
            }
            match std::fs::metadata(root) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                result => {
                    result.with_context(|| format!("Cline root {}", root.display()))?;
                }
            }
            for dir in std::fs::read_dir(root)? {
                crate::ingest::check_capture_cancelled()?;
                let dir = dir?;
                if !dir.file_type()?.is_dir() {
                    continue;
                }
                let Some(sid) = dir.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                if sid == "." || sid == ".." {
                    continue;
                }
                let path = dir.path().join(format!("{sid}.messages.json"));
                if env.path_scope().is_some_and(|s| !s.mentions(&path)) {
                    continue;
                }
                match std::fs::symlink_metadata(&path) {
                    Ok(m) if m.file_type().is_file() => {}
                    Ok(_) => continue,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                }
                let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&path)?;
                out.push(Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: Some(sid),
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
        crate::discover::ShallowReadAccess::Catalog
    }
    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        conn: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let path = Path::new(&candidate.locator);
        let snapshot = artifact(path, &AcquisitionContext::new(ID, scan.provider_roots))?;
        scan.note_open();
        scan.note_bytes(snapshot.bytes as u64);
        scan.note_records(snapshot.records as u64);
        let payload = &snapshot.payload;
        let sid = format::string(payload, "sessionId")?;
        if let Some(conn) = conn {
            normalize::check_identity(conn, sid, path)?;
        }
        let messages = payload["messages"]
            .as_array()
            .context("CLINE_INVALID_RECORD: messages")?;
        let updated = format::time(payload)?;
        let times = format::message_times(messages, updated);
        let first = times.first().map_or(updated, |time| time.0);
        let last = times.last().map_or(updated, |time| time.0);
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid.into(),
            first_prompt: format::first_prompt(messages),
            first_activity_ms: Some(first),
            last_activity_ms: Some(last.max(updated)),
            raw_path: Some(candidate.locator.clone()),
            source_stamp: Some(candidate.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let snapshot = ctx.provider_session.context("Cline snapshot missing")?;
    normalize::write(
        ctx.conn,
        &snapshot.payload,
        ctx.path.context("Cline path missing")?,
    )?;
    Ok((
        IngestOutcome {
            records: snapshot.records,
            ..Default::default()
        },
        Vec::new(),
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
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE source='cline' AND session_id=?1)",
        [sid],
        |r| r.get(0),
    )?)
}
fn sweep(ctx: SweepContext<'_>) -> Result<SweepResult> {
    let env = DiscoveryEnv::with_provider_roots(ctx.conn, ctx.roots.clone())
        .with_path_scope(ctx.paths.cloned());
    let mut result = SweepResult::default();
    let cache_key = "cline_file_stamps";
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
        if let Some(previous) = cache.get(&candidate.locator) {
            if cache_hit(ctx.conn, &candidate, previous, ctx.repairs)? {
                continue;
            }
        }
        let path = Path::new(&candidate.locator);
        let attempt = || -> Result<(usize, String)> {
            let snapshot = artifact(path, &AcquisitionContext::new(ID, ctx.roots))?;
            let tx = ctx.conn.unchecked_transaction()?;
            let n = normalize::write(&tx, &snapshot.payload, path)?;
            tx.commit()?;
            Ok((n, format::string(&snapshot.payload, "sessionId")?.into()))
        };
        match attempt() {
            Ok((n, sid)) => {
                result.inserted += n;
                cache.insert(
                    candidate.locator,
                    serde_json::json!({"stamp":candidate.stamp,"generation":GENERATION,"sid":sid}),
                );
            }
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("Cline {}: {error:#}", path.display());
                cache.remove(&candidate.locator);
                result.unread += 1;
            }
        }
    }
    if ctx.paths.is_none() && result.unread == 0 {
        retire_missing(ctx.conn, ctx.roots)?;
    }
    ctx.state.insert(cache_key.into(), Value::Object(cache));
    Ok(result)
}
fn retire_missing(conn: &Connection, roots: &ProviderRoots) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT session_id, raw_path, 1 FROM sessions WHERE source='cline' AND raw_path IS NOT NULL \
         UNION SELECT session_id, raw_locator, 0 FROM session_observations WHERE source='cline' AND location='local' AND raw_locator IS NOT NULL \
         UNION SELECT session_id, raw_locator, 0 FROM session_presences WHERE source='cline' AND location='local' AND raw_locator IS NOT NULL"
    )?;
    let paths = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)? != 0,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut by_session: std::collections::BTreeMap<String, Vec<PathBuf>> =
        std::collections::BTreeMap::new();
    for (sid, raw, catalog) in paths {
        let path = PathBuf::from(raw);
        // A remote intake may own the catalog path. Only a native-layout path
        // under a configured local root is evidence for local deletion.
        let local = roots
            .for_provider(ID)
            .iter()
            .any(|root| path.parent().and_then(Path::parent) == Some(root.as_path()));
        if catalog && !local {
            continue;
        }
        by_session.entry(sid).or_default().push(path);
    }
    for (sid, paths) in by_session {
        crate::ingest::check_capture_cancelled()?;
        let mut all_missing = true;
        for path in paths {
            let native =
                path.file_name().and_then(|s| s.to_str()) == Some(&format!("{sid}.messages.json"));
            if !native
                || !crate::providers::file_history::confirmed_missing_with_extension(
                    ID, roots, &path, "json",
                )?
            {
                all_missing = false;
                break;
            }
        }
        if all_missing {
            let tx = conn.unchecked_transaction()?;
            crate::providers::file_history::retire_local_session(&tx, ID, &sid)?;
            tx.commit()?;
        }
    }
    Ok(())
}
