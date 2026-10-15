//! Passive local Grok Bot transcript replicas. The Electron store is a bounded
//! tail, so every capture adds native entry IDs without retiring older rows.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;
use std::io::{Read, Seek};

mod format;
mod normalize;

const ID: &str = "grok-bot";
const GENERATION: &str = "grok_bot_v1";
const HEAD_LIMIT: u64 = 8 * 1024;
const TAIL_LIMIT: u64 = 32 * 1024;
const REPLICA_LIMIT: u64 = 2 * 1024 * 1024;
const ROSTER_LIMIT: u64 = 512 * 1024;

pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| Box::new(Provider)),
    hydration: Hydration::Parser(ingest),
    transcript_roots: |roots| roots.for_provider(ID).to_vec(),
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
    message_ids: MessageIdOrigin::Mixed,
    usage: None,
};
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: GENERATION,
    acquire,
    sweep,
    resolve_roots,
    watch_roots: |roots| {
        roots
            .for_provider(ID)
            .iter()
            .cloned()
            .map(WatchRoot::tree)
            .collect()
    },
});

#[derive(Clone, Copy)]
#[allow(dead_code)] // All variants are exercised by the cross-platform root test.
enum DesktopPlatform {
    Macos,
    Windows,
    Linux,
}

fn resolve_roots(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    #[cfg(target_os = "macos")]
    let platform = DesktopPlatform::Macos;
    #[cfg(target_os = "windows")]
    let platform = DesktopPlatform::Windows;
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let platform = DesktopPlatform::Linux;
    resolve_roots_for_platform(home, use_env, platform)
}

fn resolve_roots_for_platform(
    home: &Path,
    use_env: bool,
    platform: DesktopPlatform,
) -> ResolvedProviderRoots {
    let override_dir = use_env
        .then(|| std::env::var("GROK_BOT_USER_DATA").ok())
        .flatten()
        .filter(|value| !value.is_empty());
    let user_data = match override_dir {
        Some(value) if value == "~" => home.to_path_buf(),
        Some(value) if value.starts_with("~/") => home.join(&value[2..]),
        Some(value) => PathBuf::from(value),
        None => default_user_data(home, use_env, platform),
    };
    vec![user_data.join("sand-client-persistence")].into()
}

fn default_user_data(home: &Path, use_env: bool, platform: DesktopPlatform) -> PathBuf {
    match platform {
        DesktopPlatform::Macos => home.join("Library/Application Support/Grok Bot"),
        DesktopPlatform::Windows => use_env
            .then(|| std::env::var_os("APPDATA"))
            .flatten()
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("AppData/Roaming"))
            .join("Grok Bot"),
        DesktopPlatform::Linux => use_env
            .then(|| std::env::var_os("XDG_CONFIG_HOME"))
            .flatten()
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .unwrap_or_else(|| home.join(".config"))
            .join("Grok Bot"),
    }
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

fn candidate_stamp(path: &Path, slot: &str) -> Result<(String, Option<i64>)> {
    let (mut stamp, modified) = crate::file_stamp_and_modified(path)?;
    let roster = format::roster_path(path, slot);
    if roster.exists() {
        let (roster_stamp, _) = crate::file_stamp_and_modified(&roster)?;
        stamp.push(':');
        stamp.push_str(&roster_stamp);
    }
    Ok((stamp, modified))
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
            EvidenceKind::SessionMarker,
            EvidenceKind::Relationship,
        ]
    }
    fn enumerate(&self, env: &DiscoveryEnv<'_>, _: Option<usize>) -> Result<Vec<Candidate>> {
        let mut out = Vec::new();
        for root in env.provider_roots.for_provider(ID) {
            if env.path_scope().is_some_and(|scope| !scope.mentions(root)) {
                continue;
            }
            match std::fs::read_dir(root) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error).with_context(|| format!("Grok Bot root {}", root.display()))
                }
                Ok(entries) => {
                    for entry in entries {
                        crate::ingest::check_capture_cancelled()?;
                        let path = entry?.path();
                        let Some((slot, _)) = format::replica_identity(&path) else {
                            continue;
                        };
                        if !path.is_file() {
                            continue;
                        }
                        let (stamp, recency_hint_ms) = candidate_stamp(&path, &slot)?;
                        out.push(Candidate {
                            source: ID,
                            locator: path.to_string_lossy().into_owned(),
                            session_id: None,
                            recency_hint_ms,
                            stamp,
                        });
                    }
                }
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
        let (_, sid) = format::replica_identity(path).context("GROK_BOT_INVALID_KEY")?;
        let (head, tail) = read_window(&AcquisitionContext::new(ID, scan.provider_roots), path)?;
        scan.note_open();
        scan.note_bytes((head.len() + tail.len()) as u64);
        scan.note_records(1);
        anyhow::ensure!(
            format::number_hint(&head, "schemaVersion") == Some(1),
            "GROK_BOT_UNSUPPORTED_SCHEMA: transcript replica version 1 required"
        );
        if let Some(conn) = conn {
            normalize::check_identity(conn, &sid, path)?;
        }
        let last = if tail.is_empty() { &head } else { &tail };
        let persisted = format::number_hint(last, "persistedAt");
        let last_entry = format::last_entry(last);
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid,
            first_activity_ms: format::first_number_hint(&head, "timestampMs"),
            last_activity_ms: last_entry
                .as_ref()
                .and_then(format::last_activity)
                .or_else(|| format::number_hint(last, "timestampMs")),
            raw_path: Some(candidate.locator.clone()),
            source_stamp: Some(format!("{}:{persisted:?}", candidate.stamp)),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}

fn snapshot(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let (slot, sid) = format::replica_identity(path).context("GROK_BOT_INVALID_KEY")?;
    context.validate(path)?;
    anyhow::ensure!(
        std::fs::metadata(path)?.len() <= REPLICA_LIMIT,
        "GROK_BOT_REPLICA_TOO_LARGE"
    );
    let bytes = context.read(path)?;
    let replica: Value =
        serde_json::from_slice(&bytes).context("GROK_BOT_INVALID_REPLICA: JSON")?;
    let count = format::valid_replica(&replica)?.len() as i64;
    let roster_path = format::roster_path(path, &slot);
    let mut paths = vec![path.to_path_buf()];
    let (roster, roster_bytes) = if roster_path.exists() {
        context.validate(&roster_path)?;
        anyhow::ensure!(
            std::fs::metadata(&roster_path)?.len() <= ROSTER_LIMIT,
            "GROK_BOT_ROSTER_TOO_LARGE"
        );
        let bytes = context.read(&roster_path)?;
        let roster: Value =
            serde_json::from_slice(&bytes).context("GROK_BOT_INVALID_ROSTER: JSON")?;
        anyhow::ensure!(
            roster["schemaVersion"] == 4,
            "GROK_BOT_UNSUPPORTED_SCHEMA: roster version 4 required"
        );
        paths.push(roster_path);
        (Some(roster), bytes.len())
    } else {
        (None, 0)
    };
    Ok(AcquiredSession::new(
        serde_json::json!({"session_id":sid,"replica":replica,"roster":roster}),
        (bytes.len() + roster_bytes) as i64,
        count,
        paths,
    ))
}

fn acquire(
    options: &HydrateSessionOptions,
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<AcquiredSession> {
    let snapshot = snapshot(path, context)?;
    anyhow::ensure!(
        snapshot.payload["session_id"].as_str() == Some(&options.session_id),
        "SESSION_SOURCE_MISMATCH: Grok Bot native identity changed"
    );
    Ok(snapshot)
}

fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let snapshot = ctx
        .provider_session
        .context("Grok Bot acquisition snapshot missing")?;
    normalize::write(
        ctx.conn,
        &snapshot.payload,
        ctx.path.context("Grok Bot path missing")?,
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

fn unchanged(ctx: &SweepContext<'_>, cached: Option<&Value>, stamp: &str) -> Result<bool> {
    let Some(cached) = cached else {
        return Ok(false);
    };
    let Some(sid) = cached["sid"].as_str() else {
        return Ok(false);
    };
    if cached["stamp"].as_str() != Some(stamp)
        || cached["generation"].as_str() != Some(GENERATION)
        || ctx.repairs.contains(ID, sid)
    {
        return Ok(false);
    }
    Ok(ctx.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE source='grok-bot' AND session_id=?1)",
        [sid],
        |row| row.get(0),
    )?)
}

fn sweep(ctx: SweepContext<'_>) -> Result<SweepResult> {
    let env = DiscoveryEnv::with_provider_roots(ctx.conn, ctx.roots.clone())
        .with_path_scope(ctx.paths.cloned());
    let mut result = SweepResult::default();
    let cache_key = "grok_bot_file_stamps";
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
        let stamp = match read_window(&AcquisitionContext::new(ID, ctx.roots), path) {
            Ok((head, tail)) => format!(
                "{}:{:?}",
                candidate.stamp,
                format::number_hint(if tail.is_empty() { &head } else { &tail }, "persistedAt")
            ),
            Err(error) => {
                eprintln!("Grok Bot {}: {error:#}", path.display());
                result.unread += 1;
                continue;
            }
        };
        if unchanged(&ctx, cache.get(&candidate.locator), &stamp)? {
            continue;
        }
        let ingest = || -> Result<(usize, String)> {
            let snapshot = snapshot(path, &AcquisitionContext::new(ID, ctx.roots))?;
            let tx = ctx.conn.unchecked_transaction()?;
            let inserted = normalize::write(&tx, &snapshot.payload, path)?;
            tx.commit()?;
            Ok((
                inserted,
                format::string(&snapshot.payload, "session_id")?.to_owned(),
            ))
        };
        match ingest() {
            Ok((inserted, sid)) => {
                result.inserted += inserted;
                cache.insert(
                    candidate.locator.clone(),
                    serde_json::json!({"stamp":stamp,"generation":GENERATION,"sid":sid}),
                );
            }
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("Grok Bot {}: {error:#}", path.display());
                cache.remove(&candidate.locator);
                result.unread += 1;
            }
        }
    }
    ctx.state.insert(cache_key.into(), Value::Object(cache));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_roots_covers_desktop_platforms() {
        let home = Path::new("/fixture-home");
        for (platform, relative) in [
            (
                DesktopPlatform::Macos,
                "Library/Application Support/Grok Bot/sand-client-persistence",
            ),
            (
                DesktopPlatform::Windows,
                "AppData/Roaming/Grok Bot/sand-client-persistence",
            ),
            (
                DesktopPlatform::Linux,
                ".config/Grok Bot/sand-client-persistence",
            ),
        ] {
            let roots = resolve_roots_for_platform(home, false, platform);
            assert_eq!(roots.paths, vec![home.join(relative)]);
        }
    }
}
